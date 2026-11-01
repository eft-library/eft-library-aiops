"""Fixed environment account; bounded in-memory sessions, signed HttpOnly cookies."""
import hashlib
import hmac
import secrets
import threading
import time
from collections import deque

COOKIE='aiops_session'


class WebAuth:
    def __init__(self, username, password, secret):
        if not username or not password or len(secret)<32:
            raise ValueError('Web username/password and a session secret of at least 32 characters are required')
        self.username=username.encode()
        self.password=hashlib.sha256(password.encode()).digest()
        self.secret=secret.encode()
        self.sessions={}
        self.attempts={}
        self.lock=threading.Lock()

    def valid(self, cookie):
        if not cookie or len(cookie)>200:return False
        try:cookie.encode('ascii')
        except UnicodeError:return False
        sid,sep,signature=cookie.partition('.')
        expected=hmac.new(self.secret,sid.encode(),hashlib.sha256).hexdigest()
        if not sep or not hmac.compare_digest(signature,expected):return False
        with self.lock:
            expires=self.sessions.get(sid,0)
            if expires<=time.time():
                self.sessions.pop(sid,None)
                return False
            return True

    def login(self, username,password,ip):
        now=time.time()
        with self.lock:
            self.attempts={key:values for key,values in self.attempts.items() if values and values[-1]>now-60}
            if ip not in self.attempts and len(self.attempts)>=1000:return None,429
            attempts=self.attempts.setdefault(ip,deque(maxlen=10))
            while attempts and attempts[0]<=now-60:attempts.popleft()
            if len(attempts)>=10:return None,429
            attempts.append(now)
            user_ok=hmac.compare_digest(username.encode(),self.username)
            password_ok=hmac.compare_digest(hashlib.sha256(password.encode()).digest(),self.password)
            if not (user_ok and password_ok):return None,401
            self.attempts.pop(ip,None)
            self.sessions={sid:expires for sid,expires in self.sessions.items() if expires>now}
            if len(self.sessions)>=32:
                self.sessions.pop(min(self.sessions,key=self.sessions.get))
            sid=secrets.token_urlsafe(32)
            self.sessions[sid]=now+8*3600
        signature=hmac.new(self.secret,sid.encode(),hashlib.sha256).hexdigest()
        return sid+'.'+signature,200

    def logout(self,cookie):
        if cookie:
            with self.lock:self.sessions.pop(cookie.partition('.')[0],None)
