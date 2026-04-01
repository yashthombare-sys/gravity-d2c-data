#!/usr/bin/env python3
"""
encrypt_dashboard.py — Encrypt dashboard.html → index.html

Uses AES-256-GCM with PBKDF2 key derivation.
The encrypted index.html is served by GitHub Pages — the dashboard
content is only visible after entering the correct password.

Password source: DASHBOARD_PASSWORD environment variable.
"""

import os
import sys
import base64
import hashlib
import gzip

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DASHBOARD = os.path.join(SCRIPT_DIR, "dashboard.html")
INDEX = os.path.join(SCRIPT_DIR, "index.html")
ITERATIONS = 100_000


def main():
    password = os.environ.get("DASHBOARD_PASSWORD")
    if not password:
        print("⚠️  DASHBOARD_PASSWORD not set — skipping encryption")
        print("   Set it: export DASHBOARD_PASSWORD='yourpassword'")
        return

    if not os.path.exists(DASHBOARD):
        print("⚠️  dashboard.html not found — nothing to encrypt")
        return

    with open(DASHBOARD, "r", encoding="utf-8") as f:
        html = f.read()

    # Inject GitHub sync token if available (kept out of source for push protection)
    gh_token = os.environ.get("GH_SYNC_TOKEN", "")
    if gh_token and "__GH_SYNC_TOKEN__" in html:
        html = html.replace("__GH_SYNC_TOKEN__", gh_token)
        print("   Injected GH_SYNC_TOKEN into dashboard")
    elif "__GH_SYNC_TOKEN__" in html and not gh_token:
        print("   ⚠️  GH_SYNC_TOKEN not set — Sync Now button won't work")

    raw_kb = len(html) // 1024
    print(f"   Encrypting dashboard ({raw_kb} KB)...")

    # 1. Compress (gzip)
    compressed = gzip.compress(html.encode("utf-8"), compresslevel=9)
    comp_kb = len(compressed) // 1024
    print(f"   Compressed: {comp_kb} KB")

    # 2. Key derivation — PBKDF2-HMAC-SHA256
    salt = os.urandom(16)
    key = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ITERATIONS)

    # 3. AES-256-GCM encryption
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    nonce = os.urandom(12)
    aesgcm = AESGCM(key)
    ciphertext = aesgcm.encrypt(nonce, compressed, None)

    # 4. Base64-encode for embedding in HTML
    salt_b64 = base64.b64encode(salt).decode()
    nonce_b64 = base64.b64encode(nonce).decode()
    ct_b64 = base64.b64encode(ciphertext).decode()

    # 5. Build the encrypted page
    page = build_encrypted_page(salt_b64, nonce_b64, ct_b64)

    with open(INDEX, "w", encoding="utf-8") as f:
        f.write(page)

    out_kb = len(page) // 1024
    print(f"   ✅ index.html created ({out_kb} KB encrypted)")


def build_encrypted_page(salt, nonce, ciphertext):
    """Build an HTML page with encrypted dashboard content."""
    return (
        '<!DOCTYPE html>\n'
        '<html lang="en">\n'
        '<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        '<title>ClapStore Toys — Dashboard</title>\n'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">\n'
        '<style>\n'
        '*{margin:0;padding:0;box-sizing:border-box}\n'
        'body{font-family:"Inter",sans-serif;background:#0a0d12;color:#e8ecf1;'
        'min-height:100vh;display:flex;align-items:center;justify-content:center}\n'
        '.g{background:#141820;border:1px solid #1f2937;border-radius:16px;'
        'padding:40px;text-align:center;width:380px}\n'
        '.g h2{color:#FCBB13;font-size:22px;margin-bottom:8px}\n'
        '.g p{color:#8a94a6;font-size:14px;margin-bottom:24px}\n'
        '.g input{width:100%;padding:12px 16px;border-radius:8px;border:1px solid #1f2937;'
        'background:#0a0d12;color:#e8ecf1;font-size:15px;outline:none;margin-bottom:16px}\n'
        '.g input:focus{border-color:#FCBB13}\n'
        '.g button{width:100%;padding:12px;border:none;border-radius:8px;'
        'background:linear-gradient(135deg,#FCBB13,#d9a00e);color:#0a0d12;'
        'font-size:15px;font-weight:700;cursor:pointer}\n'
        '.g button:hover{opacity:0.9}\n'
        '.g button:disabled{opacity:0.5;cursor:wait}\n'
        '.e{color:#ef4444;font-size:13px;margin-top:10px;display:none}\n'
        '.i{color:#8a94a6;font-size:12px;margin-top:16px}\n'
        '.lk{font-size:48px;margin-bottom:16px}\n'
        '</style>\n'
        '</head>\n'
        '<body>\n'
        '<div class="g">\n'
        '  <div class="lk">&#128274;</div>\n'
        '  <h2>ClapStore Dashboard</h2>\n'
        '  <p>Enter password to access</p>\n'
        '  <input type="password" id="p" placeholder="Password" '
        'onkeydown="if(event.key===\'Enter\')U()">\n'
        '  <button id="b" onclick="U()">Unlock</button>\n'
        '  <div class="e" id="e">Incorrect password</div>\n'
        '  <p class="i">AES-256-GCM encrypted &middot; Secure access only</p>\n'
        '</div>\n'
        '<script>\n'
        'const _S="' + salt + '",_N="' + nonce + '",_I=' + str(ITERATIONS) + ';\n'
        'const _C="' + ciphertext + '";\n'
        'function D(s){return Uint8Array.from(atob(s),c=>c.charCodeAt(0))}\n'
        'async function U(){\n'
        '  const b=document.getElementById("b"),e=document.getElementById("e"),'
        'pw=document.getElementById("p").value;\n'
        '  if(!pw)return;\n'
        '  b.disabled=true;b.textContent="Decrypting...";e.style.display="none";\n'
        '  try{\n'
        '    const km=await crypto.subtle.importKey("raw",'
        'new TextEncoder().encode(pw),"PBKDF2",false,["deriveKey"]);\n'
        '    const k=await crypto.subtle.deriveKey('
        '{name:"PBKDF2",salt:D(_S),iterations:_I,hash:"SHA-256"},'
        'km,{name:"AES-GCM",length:256},false,["decrypt"]);\n'
        '    const dec=await crypto.subtle.decrypt({name:"AES-GCM",iv:D(_N)},k,D(_C));\n'
        '    const ds=new DecompressionStream("gzip");\n'
        '    const w=ds.writable.getWriter();w.write(new Uint8Array(dec));w.close();\n'
        '    const r=ds.readable.getReader();const ch=[];\n'
        '    while(true){const{done,value}=await r.read();if(done)break;ch.push(value)}\n'
        '    const html=new TextDecoder().decode(await new Blob(ch).arrayBuffer());\n'
        '    sessionStorage.setItem("dash_auth","1");\n'
        '    sessionStorage.setItem("_dp",pw);\n'
        '    document.open();document.write(html);document.close();\n'
        '  }catch(x){\n'
        '    e.style.display="block";b.disabled=false;b.textContent="Unlock";\n'
        '  }\n'
        '}\n'
        '(function(){const c=sessionStorage.getItem("_dp");\n'
        'if(c){document.getElementById("p").value=c;U()}})();\n'
        '</script>\n'
        '</body>\n'
        '</html>'
    )


if __name__ == "__main__":
    main()
