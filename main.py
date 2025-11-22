#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import yt_dlp

def check_ffmpeg():
    if sys.platform == 'win32':
        import winreg
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment") as key:
                system_path = winreg.QueryValueEx(key, "PATH")[0]
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
                user_path = winreg.QueryValueEx(key, "PATH")[0]
            os.environ['PATH'] = system_path + os.pathsep + user_path
        except Exception:
            pass
    return shutil.which('ffmpeg') is not None

def normalize_title(title):
    if not title:
        return ""
    title = title.lower().strip()
    title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_', '.'))
    return title

def get_playlist_urls(url):
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True,
        'ignoreerrors': True,
    }
    
    urls = []
    seen_urls = set()
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if 'entries' in info:
                for entry in info['entries']:
                    if entry and 'url' in entry:
                        track_url = entry['url']
                        if track_url not in seen_urls:
                            seen_urls.add(track_url)
                            urls.append(track_url)
    except Exception as e:
        print(f"Ошибка при получении списка треков: {e}")
    
    return urls

def get_track_info(track_url):
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'ignoreerrors': True,
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(track_url, download=False)
            if info and 'title' in info:
                return info['title']
    except Exception:
        pass
    
    return None

def file_exists(output_path, track_title):
    if not track_title:
        return False
    
    normalized_title = normalize_title(track_title)
    
    if not output_path.exists():
        return False
    
    for file_path in output_path.glob('*'):
        if file_path.is_file():
            file_stem = normalize_title(file_path.stem)
            if normalized_title == file_stem or normalized_title in file_stem or file_stem in normalized_title:
                if len(normalized_title) > 5 and len(file_stem) > 5:
                    return True
    
    return False

def download_track(track_url, output_path, index, total, retry_count=3, use_ffmpeg=True, skip_existing=True, existing_titles=None):
    if skip_existing and existing_titles is not None:
        track_title = get_track_info(track_url)
        if track_title:
            normalized = normalize_title(track_title)
            if normalized and normalized in existing_titles:
                return True, f"[{index}/{total}] Пропущен (уже существует)"
            if file_exists(output_path, track_title):
                return True, f"[{index}/{total}] Пропущен (уже существует)"
    
    for attempt in range(retry_count):
        ydl_opts = {
            'format': 'bestaudio[ext=m4a]/bestaudio/best',
            'outtmpl': str(output_path / '%(title)s.%(ext)s'),
            'embedthumbnail': False,
            'writethumbnail': False,
            'ignoreerrors': True,
            'quiet': True,
            'no_warnings': True,
            'extractor_args': {'soundcloud': {'skip_preview': True}},
            'retries': 10,
            'fragment_retries': 20,
            'file_access_retries': 5,
            'concurrent_fragments': 4,
            'hls_prefer_native': True,
            'keep_fragments': False,
            'noprogress': False,
            'socket_timeout': 30,
            'http_chunk_size': 10485760,
        }
        
        if use_ffmpeg and check_ffmpeg():
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '320',
            }]
            ydl_opts['outtmpl'] = str(output_path / '%(title)s.%(ext)s')
        
        try:
            download_complete = False
            downloaded_file = None
            expected_filename = None
            
            def progress_hook(d):
                nonlocal download_complete, downloaded_file
                if d.get('status') == 'finished':
                    download_complete = True
                    downloaded_file = d.get('filename')
            
            ydl_opts['progress_hooks'] = [progress_hook]
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(track_url, download=False)
                duration = info.get('duration', 0) if info else 0
                filesize = info.get('filesize') or info.get('filesize_approx', 0)
                track_title = info.get('title', '') if info else ''
                
                if duration > 0 and duration < 60:
                    return False, f"[{index}/{total}] Пропущен (превью {int(duration)}с, не полная версия)"
                
                if track_title:
                    safe_title = "".join(c for c in track_title if c.isalnum() or c in (' ', '-', '_', '.')).strip()
                    if use_ffmpeg and check_ffmpeg():
                        expected_filename = f"{safe_title}.mp3"
                    else:
                        ext = info.get('ext', 'm4a')
                        expected_filename = f"{safe_title}.{ext}"
                
                ydl.download([track_url])
                
                time.sleep(0.1)
                
                found_file = None
                if downloaded_file:
                    found_file = Path(downloaded_file)
                elif expected_filename:
                    found_file = output_path / expected_filename
                    if not found_file.exists():
                        for file_path in output_path.glob('*'):
                            if safe_title.lower() in normalize_title(file_path.stem):
                                found_file = file_path
                                break
                
                if found_file and found_file.exists():
                    actual_size = found_file.stat().st_size
                    if filesize > 0:
                        if actual_size < filesize * 0.85:
                            found_file.unlink()
                            return False, f"[{index}/{total}] Файл неполный ({actual_size}/{filesize} байт, {actual_size*100//filesize}%)"
                    elif actual_size < 100000:
                        found_file.unlink()
                        return False, f"[{index}/{total}] Файл слишком маленький ({actual_size} байт)"
                elif not download_complete:
                    return False, f"[{index}/{total}] Скачивание не завершено"
                
                if use_ffmpeg and check_ffmpeg():
                    for file_path in output_path.glob('*.mp3.mp3'):
                        name_without_ext = file_path.stem
                        if name_without_ext.endswith('.mp3'):
                            name_without_ext = name_without_ext[:-4]
                        new_path = file_path.parent / f"{name_without_ext}.mp3"
                        try:
                            if new_path.exists():
                                file_path.unlink()
                            else:
                                file_path.rename(new_path)
                        except Exception:
                            pass
                
                duration_str = f" ({int(duration)}с)" if duration > 0 else ""
                return True, f"[{index}/{total}] Успешно скачан{duration_str}"
        except yt_dlp.utils.DownloadError as e:
            error_str = str(e)
            
            if 'ffmpeg' in error_str.lower() or 'ffprobe' in error_str.lower():
                if attempt == 0 and use_ffmpeg:
                    return download_track(track_url, output_path, index, total, retry_count, use_ffmpeg=False, skip_existing=skip_existing, existing_titles=existing_titles)
                else:
                    return False, f"[{index}/{total}] Ошибка конвертации (ffmpeg не найден)"
            elif 'geo restriction' in error_str.lower() or 'not available from your location' in error_str.lower():
                return False, f"[{index}/{total}] Гео-ограничение"
            elif '429' in error_str or 'Too Many Requests' in error_str:
                if attempt < retry_count - 1:
                    wait_time = (attempt + 1) * 2
                    time.sleep(wait_time)
                    continue
                return False, f"[{index}/{total}] Слишком много запросов (429)"
            else:
                if attempt < retry_count - 1:
                    time.sleep(1)
                    continue
                return False, f"[{index}/{total}] Ошибка: {error_str[:60]}"
        except Exception as e:
            if attempt < retry_count - 1:
                time.sleep(1)
                continue
            return False, f"[{index}/{total}] Ошибка: {str(e)[:60]}"
    
    return False, f"[{index}/{total}] Не удалось скачать после {retry_count} попыток"

def download_soundcloud_likes(url, output_dir="downloads", max_workers=5):
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    print(f"Получаю список треков с: {url}")
    
    has_ffmpeg = check_ffmpeg()
    if has_ffmpeg:
        print("FFmpeg найден - файлы будут конвертированы в MP3")
    else:
        print("FFmpeg не найден - файлы будут скачаны в исходном формате")
        print("Для конвертации в MP3 установите ffmpeg: https://ffmpeg.org/download.html")
    print("-" * 50)
    
    track_urls = get_playlist_urls(url)
    track_urls = list(dict.fromkeys(track_urls))
    
    if not track_urls:
        print("Не удалось получить список треков. Попробую скачать напрямую...")
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': str(output_path / '%(title)s.%(ext)s'),
            'embedthumbnail': False,
            'writethumbnail': False,
            'ignoreerrors': True,
            'quiet': False,
            'retries': 10,
            'fragment_retries': 20,
            'file_access_retries': 5,
            'concurrent_fragments': 4,
            'hls_prefer_native': True,
            'keep_fragments': False,
            'noprogress': False,
            'socket_timeout': 30,
            'http_chunk_size': 10485760,
        }
        
        if check_ffmpeg():
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '320',
            }]
            ydl_opts['outtmpl'] = str(output_path / '%(title)s.%(ext)s')
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return
    
    total = len(track_urls)
    print(f"Найдено уникальных треков: {total}")
    
    existing_titles = set()
    if output_path.exists():
        for file_path in output_path.glob('*'):
            if file_path.is_file():
                normalized = normalize_title(file_path.stem)
                if normalized:
                    existing_titles.add(normalized)
    
    if existing_titles:
        print(f"Найдено уже скачанных файлов: {len(existing_titles)}")
    
    print(f"Треки будут сохранены в папку: {output_path.absolute()}")
    print(f"Параллельных загрузок: {max_workers}")
    print("-" * 50)
    
    successful = 0
    failed = 0
    skipped = 0
    geo_restricted = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        use_ffmpeg = check_ffmpeg()
        future_to_url = {
            executor.submit(download_track, track_url, output_path, i+1, total, 3, use_ffmpeg, True, existing_titles): track_url
            for i, track_url in enumerate(track_urls)
        }
        
        for future in as_completed(future_to_url):
            success, message = future.result()
            print(message)
            if success:
                if 'пропущен' in message.lower() or 'уже существует' in message.lower():
                    skipped += 1
                else:
                    successful += 1
            else:
                failed += 1
                if 'гео-ограничение' in message.lower() or 'geo restriction' in message.lower():
                    geo_restricted += 1
    
    print("-" * 50)
    print(f"Скачивание завершено!")
    print(f"Скачано новых: {successful}, Пропущено: {skipped}, Ошибок: {failed}, Всего: {total}")
    if geo_restricted > 0:
        print(f"Гео-ограничений: {geo_restricted}")

def main():
    if len(sys.argv) < 2:
        print("Использование: python main.py <URL> [output_dir] [max_workers]")
        print("Пример: python main.py https://soundcloud.com/user/likes downloads 5")
        sys.exit(1)
    
    likes_url = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "downloads"
    max_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    
    print("=" * 50)
    print("MyLikes Downloader")
    print("=" * 50)
    
    download_soundcloud_likes(likes_url, output_dir, max_workers)

if __name__ == "__main__":
    main()
