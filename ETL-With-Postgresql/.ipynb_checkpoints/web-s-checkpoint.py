import requests

session = requests.session()

session.proxies = {}
session.proxies['http'] = 'socks5h://54.237.103.199:9050'
session.proxies['https'] = 'socks5h://54.237.103.199:9050'

r = session.get("http://httpbin.org/ip")
print(r.text)
