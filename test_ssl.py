import ssl
import urllib.request

try:
    response = urllib.request.urlopen('https://www.google.com')
    print("✅ SSL working! Status:", response.getcode())
except Exception as e:
    print("❌ SSL still broken:", e)