'use strict';
document.getElementById('login-form').addEventListener('submit',async event=>{
 event.preventDefault();const form=event.currentTarget,button=form.querySelector('button'),error=document.getElementById('login-error');button.disabled=true;error.textContent='';
 try{
  const response=await fetch('/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(Object.fromEntries(new FormData(form)))});
  if(!response.ok){error.textContent=response.status===429?'시도가 많습니다. 잠시 후 다시 로그인하세요.':'아이디와 비밀번호를 확인하세요.';return;}
  location.replace('/'+location.hash);
 }catch{error.textContent='서버에 연결할 수 없습니다.';}finally{button.disabled=false;}
});
