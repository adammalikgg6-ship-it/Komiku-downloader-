# downloader.py
import os
import requests
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO

def download_chapter(chapter_url, chapter_num, OUTPUT_DIR, chat_id=None, user_cancel=None):
    print(f"[*] Mengambil gambar dari {chapter_url}")
    
    # Coba akses URL asli dulu
    resp = requests.get(chapter_url, headers={"User-Agent": "Mozilla/5.0"})
    
    # Jika gagal dan chapter adalah satuan (1-9), coba format dengan 0 di depan
    if resp.status_code != 200:
        try:
            # Cek apakah chapter_num adalah satuan tanpa 0 di depan
            if str(chapter_num).isdigit() and 1 <= int(chapter_num) <= 9:
                # Ubah URL untuk menggunakan format 01, 02, dst
                alt_chapter_url = chapter_url.replace(f"-{chapter_num}/", f"-0{chapter_num}/")
                print(f"[*] Mencoba format alternatif: {alt_chapter_url}")
                
                alt_resp = requests.get(alt_chapter_url, headers={"User-Agent": "Mozilla/5.0"})
                if alt_resp.status_code == 200:
                    resp = alt_resp
                    chapter_url = alt_chapter_url
                    print(f"[+] Berhasil dengan format 0{chapter_num}")
                else:
                    print(f"[!] Gagal mengakses kedua format untuk chapter {chapter_num}")
                    return []
            else:
                print(f"[!] Gagal mengakses {chapter_url}")
                return []
        except:
            print(f"[!] Gagal mengakses {chapter_url}")
            return []

    soup = BeautifulSoup(resp.text, "html.parser")
    img_tags = soup.select("img")
    img_urls = []

    for img in img_tags:
        src = img.get("src") or img.get("data-src")
        if src and (src.endswith(".jpg") or src.endswith(".png")):
            # Skip advertisement and non-content images
            if "komikuplus" in src or "asset/img" in src:
                continue
            if not src.startswith("http"):
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = "https://komiku.org" + src
                else:
                    src = "https://" + src
            img_urls.append(src)

    if not img_urls:
        print(f"[!] Tidak ada gambar ditemukan di {chapter_url}")
        return []

    # Skip first 3 images only (keep last image for manga mode)
    if len(img_urls) > 3:  # Only skip if we have more than 3 images
        img_urls = img_urls[3:]  # Skip first 3 images only
        print(f"    > MANGA MODE: Skipping first 3 images. Processing {len(img_urls)} images.")
    else:
        print(f"    > Too few images ({len(img_urls)}), not skipping any.")

    chapter_folder = os.path.join(OUTPUT_DIR, f"chapter-{chapter_num}")
    os.makedirs(chapter_folder, exist_ok=True)

    images = []
    for i, img_url in enumerate(img_urls, start=1):
        # Check for cancellation
        if user_cancel and chat_id and user_cancel.get(chat_id):
            print(f"[!] Download cancelled for chapter {chapter_num}")
            return []

        try:
            img_resp = requests.get(img_url, stream=True)
            img = Image.open(BytesIO(img_resp.content)).convert("RGB")
            img_path = os.path.join(chapter_folder, f"{i:03}.jpg")
            img.save(img_path, "JPEG")
            images.append(img_path)
            print(f"    > Download gambar {i}/{len(img_urls)}")
        except Exception as e:
            print(f"    [!] Gagal download {img_url}: {e}")

    return images

def download_chapter_big(chapter_url, chapter_num, OUTPUT_DIR, chat_id=None, user_cancel=None):
    """Download chapter with larger dimensions and higher quality images for /big mode"""
    print(f"[*] BIG MODE: Mengambil gambar dari {chapter_url}")
    
    # Coba akses URL asli dulu
    resp = requests.get(chapter_url, headers={"User-Agent": "Mozilla/5.0"})
    
    # Jika gagal dan chapter adalah satuan (1-9), coba format dengan 0 di depan
    if resp.status_code != 200:
        try:
            # Cek apakah chapter_num adalah satuan tanpa 0 di depan
            if str(chapter_num).isdigit() and 1 <= int(chapter_num) <= 9:
                # Ubah URL untuk menggunakan format 01, 02, dst
                alt_chapter_url = chapter_url.replace(f"-{chapter_num}/", f"-0{chapter_num}/")
                print(f"[*] BIG MODE: Mencoba format alternatif: {alt_chapter_url}")
                
                alt_resp = requests.get(alt_chapter_url, headers={"User-Agent": "Mozilla/5.0"})
                if alt_resp.status_code == 200:
                    resp = alt_resp
                    chapter_url = alt_chapter_url
                    print(f"[+] BIG MODE: Berhasil dengan format 0{chapter_num}")
                else:
                    print(f"[!] BIG MODE: Gagal mengakses kedua format untuk chapter {chapter_num}")
                    return []
            else:
                print(f"[!] Gagal mengakses {chapter_url}")
                return []
        except:
            print(f"[!] Gagal mengakses {chapter_url}")
            return []

    soup = BeautifulSoup(resp.text, "html.parser")
    img_tags = soup.select("img")
    img_urls = []

    for img in img_tags:
        src = img.get("src") or img.get("data-src")
        if src and (src.endswith(".jpg") or src.endswith(".png")):
            # Skip advertisement and non-content images
            if "komikuplus" in src or "asset/img" in src:
                continue
            if not src.startswith("http"):
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = "https://komiku.org" + src
                else:
                    src = "https://" + src

            # Try to get higher resolution image for BIG mode
            # Replace common size indicators with larger versions
            if "?resize=" in src:
                src = src.split("?resize=")[0]  # Remove resize parameter
            elif "thumb" in src:
                src = src.replace("thumb", "full")  # Replace thumb with full
            elif "_small" in src:
                src = src.replace("_small", "_large")  # Replace small with large
            elif "_medium" in src:
                src = src.replace("_medium", "_large")  # Replace medium with large

            img_urls.append(src)

    if not img_urls:
        print(f"[!] Tidak ada gambar ditemukan di {chapter_url}")
        return []

    # Skip first 3 and last 1 images for komik mode
    if len(img_urls) > 4:  # Only skip if we have more than 4 images
        img_urls = img_urls[3:-1]  # Skip first 3 and last 1
        print(f"    > KOMIK MODE: Skipping first 3 and last 1 images. Processing {len(img_urls)} images.")
    else:
        print(f"    > KOMIK MODE: Too few images ({len(img_urls)}), not skipping any.")

    chapter_folder = os.path.join(OUTPUT_DIR, f"chapter-{chapter_num}-big")
    os.makedirs(chapter_folder, exist_ok=True)

    images = []
    for i, img_url in enumerate(img_urls, start=1):
        # Check for cancellation
        if user_cancel and chat_id and user_cancel.get(chat_id):
            print(f"[!] BIG MODE download cancelled for chapter {chapter_num}")
            return []

        try:
            img_resp = requests.get(img_url, stream=True)
            img = Image.open(BytesIO(img_resp.content))

            # Get original dimensions
            original_width, original_height = img.size

            # Ensure consistent sizing for BIG mode
            # Set minimum width for consistency
            min_width = 1200
            if original_width < min_width:
                scale_factor = min_width / original_width
                new_width = min_width
                new_height = int(original_height * scale_factor)
            else:
                # For larger images, use 150% scaling
                new_width = int(original_width * 1.5)
                new_height = int(original_height * 1.5)

            # Resize using high-quality resampling
            img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

            # Convert to RGB if necessary
            if img_resized.mode != "RGB":
                img_resized = img_resized.convert("RGB")

            img_path = os.path.join(chapter_folder, f"{i:03}.jpg")
            # Save with maximum quality for BIG mode
            img_resized.save(img_path, "JPEG", quality=100, optimize=False)
            images.append(img_path)
            print(f"    > BIG MODE: Download gambar {i}/{len(img_urls)} - Ukuran: {original_width}x{original_height} → {new_width}x{new_height}")
        except Exception as e:
            print(f"    [!] Gagal download {img_url}: {e}")

    return images

def create_pdf(all_images, output_pdf):
    if not all_images:
        print("[!] Tidak ada gambar untuk dibuat PDF.")
        return

    try:
        # Process images in smaller batches to save memory
        batch_size = 10
        first_image = None
        processed_images = []

        for i in range(0, len(all_images), batch_size):
            batch = all_images[i:i + batch_size]
            batch_processed = []

            for img_path in batch:
                try:
                    img = Image.open(img_path).convert("RGB")
                    # Optimize image size if too large (reduce quality for very large images)
                    width, height = img.size
                    if width * height > 4000000:  # If image is very large (>4MP)
                        # Reduce size by 20%
                        new_width = int(width * 0.8)
                        new_height = int(height * 0.8)
                        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

                    if first_image is None:
                        first_image = img
                    else:
                        batch_processed.append(img)

                except Exception as e:
                    print(f"[!] Error processing {img_path}: {e}")
                    continue

            processed_images.extend(batch_processed)

        if first_image:
            # Save with optimization
            first_image.save(
                output_pdf, 
                save_all=True, 
                append_images=processed_images,
                optimize=True,
                quality=85  # Slightly reduce quality to save space
            )
            print(f"[+] PDF dibuat: {output_pdf}")

            # Check final file size
            file_size = os.path.getsize(output_pdf)
            print(f"[+] Ukuran PDF: {file_size/(1024*1024):.1f}MB")

            if file_size > 45 * 1024 * 1024:  # Warn if close to 50MB limit
                print(f"[!] Warning: PDF mendekati batas ukuran Telegram (45MB+)")
        else:
            print("[!] Tidak ada gambar yang bisa diproses untuk PDF.")

    except Exception as e:
        print(f"[!] Error creating PDF: {e}")
        # Try fallback method with lower quality
        try:
            pil_images = []
            for img_path in all_images[:20]:  # Limit to first 20 images as fallback
                try:
                    img = Image.open(img_path).convert("RGB")
                    # Reduce size significantly for fallback
                    width, height = img.size
                    new_width = int(width * 0.6)
                    new_height = int(height * 0.6)
                    img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    pil_images.append(img)
                except:
                    continue

            if pil_images:
                pil_images[0].save(
                    output_pdf, 
                    save_all=True, 
                    append_images=pil_images[1:],
                    optimize=True,
                    quality=70
                )
                print(f"[+] PDF fallback dibuat: {output_pdf}")

        except Exception as fallback_error:
            print(f"[!] Fallback PDF creation failed: {fallback_error}")