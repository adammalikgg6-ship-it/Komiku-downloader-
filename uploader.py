
import os
import requests
import json
import time

class GoFileUploader:
    def __init__(self):
        self.base_url = "https://api.gofile.io"
        self.server = None
        self.fallback_servers = ["store1", "store2", "store3", "store4", "store5"]
        self.get_server()
    
    def get_server(self, retry=3):
        """Get the best server for upload with retry mechanism"""
        # Try official API first
        for attempt in range(retry):
            try:
                print(f"🔍 Getting GoFile server (attempt {attempt + 1}/{retry})...")
                response = requests.get(f"{self.base_url}/servers", timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('status') == 'ok' and 'data' in data:
                        servers = data['data']
                        if isinstance(servers, dict) and servers:
                            # Get first available server
                            self.server = list(servers.keys())[0]
                            print(f"✅ GoFile server ready: {self.server}")
                            return True
                        elif isinstance(servers, list) and servers:
                            self.server = servers[0]['name']
                            print(f"✅ GoFile server ready: {self.server}")
                            return True
                    else:
                        print(f"❌ GoFile server response error: {data}")
                else:
                    print(f"❌ GoFile server HTTP error: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ GoFile server network error (attempt {attempt + 1}): {e}")
            except Exception as e:
                print(f"❌ GoFile server error (attempt {attempt + 1}): {e}")
            
            if attempt < retry - 1:
                wait_time = (attempt + 1) * 2
                print(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
        
        # If official API fails, use fallback server
        print("🔄 Using fallback server...")
        self.server = self.fallback_servers[0]  # Use store1 as default
        print(f"✅ GoFile fallback server ready: {self.server}")
        return True
    
    def upload_file(self, file_path, file_name=None):
        """Upload file to GoFile with improved error handling and correct API"""
        # Try to get server if not available
        if not self.server:
            print("🔄 Attempting to reconnect to GoFile server...")
            if not self.get_server():
                print("❌ GoFile server not available")
                return None
        
        try:
            if not file_name:
                file_name = os.path.basename(file_path)
            
            file_size = os.path.getsize(file_path)
            print(f"📤 Uploading {file_name} ({file_size/(1024*1024):.1f}MB) to GoFile...")
            
            # Updated upload URLs with correct GoFile API
            upload_attempts = [
                f"https://{self.server}.gofile.io/contents/uploadfile",
                "https://store1.gofile.io/contents/uploadfile", 
                "https://store2.gofile.io/contents/uploadfile",
                "https://store3.gofile.io/contents/uploadfile",
                "https://store4.gofile.io/contents/uploadfile"
            ]
            
            for attempt, upload_url in enumerate(upload_attempts):
                try:
                    print(f"🔄 Upload attempt {attempt + 1} to {upload_url}")
                    
                    with open(file_path, 'rb') as file_data:
                        files = {'file': (file_name, file_data, 'application/pdf')}
                        data_payload = {'folderId': ''}  # Empty folder ID for root
                        
                        # Upload with longer timeout for large files
                        timeout = max(300, file_size // (1024 * 1024) * 10)  # 10s per MB, min 5 min
                        response = requests.post(upload_url, files=files, data=data_payload, timeout=timeout)
                    
                    if response.status_code == 200:
                        try:
                            response_data = response.json()
                            if response_data.get('status') == 'ok' and 'data' in response_data:
                                file_info = response_data['data']
                                
                                # Extract download information from GoFile response
                                file_code = file_info.get('code', '')
                                download_page = file_info.get('downloadPage', f"https://gofile.io/d/{file_code}")
                                direct_link = file_info.get('link', f"https://gofile.io/d/{file_code}")
                                
                                # Alternative extraction methods
                                if not download_page and file_code:
                                    download_page = f"https://gofile.io/d/{file_code}"
                                if not direct_link and file_code:
                                    direct_link = f"https://gofile.io/d/{file_code}"
                                
                                if download_page or direct_link:
                                    print(f"✅ File uploaded to GoFile successfully: {file_name}")
                                    return {
                                        'download_page': download_page,
                                        'direct_link': direct_link,
                                        'file_name': file_name,
                                        'file_size': file_size
                                    }
                                else:
                                    print(f"❌ GoFile response missing download info: {response_data}")
                            else:
                                print(f"❌ GoFile upload failed: {response_data.get('message', 'Unknown error')}")
                        except json.JSONDecodeError as je:
                            print(f"❌ GoFile response JSON error: {je}")
                            print(f"Raw response: {response.text[:500]}")
                    else:
                        print(f"❌ GoFile upload failed with status: {response.status_code}")
                        print(f"Response: {response.text[:200]}")
                        
                except requests.exceptions.Timeout:
                    print(f"❌ Upload timeout for attempt {attempt + 1}")
                except requests.exceptions.RequestException as re:
                    print(f"❌ Upload network error for attempt {attempt + 1}: {re}")
                except Exception as ue:
                    print(f"❌ Upload error for attempt {attempt + 1}: {ue}")
                
                # Wait before next attempt
                if attempt < len(upload_attempts) - 1:
                    print("⏳ Waiting 3s before next upload attempt...")
                    time.sleep(3)
            
            print("❌ All GoFile upload attempts failed")
            return None
                
        except FileNotFoundError:
            print(f"❌ File not found: {file_path}")
            return None
        except Exception as e:
            print(f"❌ GoFile upload error: {e}")
            return None
    
    def is_available(self):
        """Check if GoFile service is available"""
        if not self.server:
            return self.get_server()
        return True
    
    def test_connection(self):
        """Test GoFile connection"""
        try:
            response = requests.get(f"{self.base_url}/getServer", timeout=5)
            return response.status_code == 200
        except:
            return False
