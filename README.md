================================
🤖 BOT MANGA DOWNLOADER README
================================

📚 TENTANG BOT INI
==================
Bot Telegram untuk download manga dari website Komiku.org dengan fitur:
- Download manga dalam format PDF
- Mode Normal dan Mode Komik (resolusi tinggi)
- Upload otomatis ke GoFile untuk file besar
- Gabung multiple chapter atau pisah per chapter
- Auto demo untuk testing
- Smart monitoring dan error recovery

🚀 FITUR UTAMA
==============
1. Mode Download:
   - Mode Normal (/manga): Download standar
   - Mode Komik (/komik): Download dengan resolusi lebih tinggi (max 3 chapter)

2. Format Output:
   - Gabung: Semua chapter jadi 1 PDF
   - Pisah: 1 PDF per chapter

3. Upload Options:
   - Telegram Direct: Max 50MB
   - GoFile: Unlimited size (file hosting gratis)

4. Commands:
   - /start - Mulai bot
   - /manga - Mode download normal
   - /komik - Mode download komik
   - /cancel - Hentikan download
   - /clear - Hapus pesan bot
   - /autodemo - Demo otomatis
   - /offautodemo - Stop demo

⚙️ CARA INSTALL DEPENDENCIES
============================
1. Buka Terminal/Shell di Replit
2. Jalankan command:
   pip install -r requirements.txt

Atau dependencies akan ter-install otomatis saat run bot.

Dependencies yang dibutuhkan:
- requests: untuk HTTP requests
- beautifulsoup4: untuk parsing HTML
- Pillow: untuk manipulasi gambar
- pyTelegramBotAPI: untuk Telegram Bot API
- flask: untuk keep-alive server
- python-dotenv: untuk environment variables

🔑 CARA DAPATKAN TOKEN BOT TELEGRAM
===================================
1. Buka Telegram dan cari @BotFather
2. Kirim command /newbot
3. Masukkan nama bot (contoh: My Manga Bot)
4. Masukkan username bot (harus diakhiri 'bot', contoh: mymangadownloader_bot)
5. BotFather akan memberikan token seperti: 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11
6. Copy token tersebut
7. Di Replit, buka tab "Secrets" (ikon gembok)
8. Buat secret baru:
   - Key: BOT_TOKEN
   - Value: paste token dari BotFather

📁 STRUKTUR FILE
================
main.py                    - File utama bot
downloader.py              - Module download manga
uploader.py                - Module upload GoFile
keep_alive.py             - Keep bot online
requirements.txt          - Dependencies list
downloads/                - Folder temporary download
.env (opsional)          - Environment variables

🔧 ENVIRONMENT VARIABLES
========================
Wajib:
- BOT_TOKEN: Token bot dari BotFather

Opsional:
- REPLIT_DEPLOYMENT: Set ke "1" untuk deployment mode

💻 CARA MENJALANKAN BOT
=======================
1. Pastikan semua dependencies ter-install
2. Set BOT_TOKEN di Secrets
3. Klik tombol "Run" atau jalankan: python main.py
4. Bot akan online dan siap digunakan

📝 CARA MENGGUNAKAN BOT
=======================
1. Start bot dengan /start
2. Pilih mode download (/manga atau /komik)
3. Kirim link halaman manga (bukan chapter)
   Contoh: https://komiku.org/manga/one-piece/
4. Masukkan chapter awal (contoh: 1)
5. Masukkan chapter akhir (contoh: 5)
6. Pilih mode output dan platform upload
7. Tunggu download selesai

🐛 TROUBLESHOOTING
==================
- Bot tidak respond: Check BOT_TOKEN di Secrets
- File too large: Gunakan opsi GoFile
- Download gagal: Check link manga valid
- Memory error: Restart bot, auto cleanup aktif
- GoFile error: Service mungkin down, coba lagi

⚡ FITUR ADVANCED
=================
- Auto cleanup: Hapus file otomatis
- Smart monitoring: Deteksi error dan auto recovery
- Conflict prevention: Hindari crash dari multiple request
- Progressive backoff: Smart retry mechanism
- Memory optimization: Batch processing untuk file besar

📤 TENTANG GOFILE
=================
GoFile adalah layanan file hosting gratis yang:
- Mendukung file hingga beberapa GB
- Tidak perlu registrasi atau API key
- Link download langsung tersedia
- Cocok untuk file besar yang melebihi batas Telegram

👨‍💻 DEVELOPER INFO
===================
- Platform: Replit (Python)
- Framework: pyTelegramBotAPI
- Storage: GoFile API
- Monitoring: Built-in error recovery
- Deployment: Ready untuk 24/7 online

📞 SUPPORT
==========
Jika ada error atau butuh bantuan:
1. Check log di console Replit
2. Restart bot dengan tombol "Run"
3. Pastikan BOT_TOKEN benar
4. Jika GoFile error, coba beberapa kali

================================
📚 HAPPY MANGA DOWNLOADING! 🤖
================================