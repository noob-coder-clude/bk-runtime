const http=require('http'),sp=require('child_process'),fs=require('fs'),net=require('net');
const APPPORT=9998, PUBPORT=8080;
const logs=[];
function DB(sql){
  if(!process.env.LOGDB_URI) return;
  try{
    const {Client}=require('pg');
    const q = process.env.LOGDB_PASS ? process.env.LOGDB_URI+'?password='+encodeURIComponent(process.env.LOGDB_PASS) : process.env.LOGDB_URI;
    const c=new Client({connectionString:q});
    c.connect().then(()=>c.query('INSERT INTO bklogs(msg) VALUES ($1)', [String(sql).slice(0,500)]).finally(()=>c.end()));
  }catch(e){}
}
function lg(s){
  const line=Date.now().toString().slice(-8)+'|'+s;
  logs.push(line); if(logs.length>150)logs.shift();
  console.log('[beacon]',s);
  DB(line);
}
process.on('uncaughtException',e=>{lg('UNCAUGHT '+(e.message||e));});
process.on('unhandledRejection',e=>{lg('UNHANDLEDREJ '+((e&&e.message)||e));});
let tries=0, appUp=false;
function startApp(){
  tries++;
  lg('spawning app try#'+tries);
  const p=sp.spawn('node',['index.js'],{cwd:'/app',env:{...process.env, PORT:String(APPPORT)},stdio:['ignore','pipe','pipe']});
  p.stdout.on('data',d=>lg('[app] '+d.toString().trim()));
  p.stderr.on('data',d=>lg('[app:err] '+d.toString().trim()));
  p.on('exit',(c,s)=>{lg('[app] exited code='+c+' signal='+s); appUp=false; setTimeout(startApp,3000);});
  p.on('error',e=>{lg('[app] spawn error '+(e.code||e));});
}
function tick(){
  const s=net.connect(APPPORT,'127.0.0.1');
  s.on('connect',()=>{ if(!appUp) lg('app port UP'); appUp=true; s.end(); });
  s.on('error',()=>{ appUp=false; });
}
startApp();
setInterval(tick,1000);
const proxy=http.createServer((req,res)=>{
  if(req.url==='/__bkdebug'){
    res.writeHead(200,{'content-type':'text/plain','cache-control':'no-store'});
    res.end('BK-BEACON try='+tries+' appUp='+appUp+'\n'+logs.join('\n')+'\n'); return;
  }
  if(!appUp){
    res.writeHead(200,{'content-type':'text/plain','retry-after':'5'});
    res.end('BK warming: app not ready yet\n'+logs.slice(-5).join('\n')+'\n'); return;
  }
  const o=http.request({host:'127.0.0.1',port:APPPORT,path:req.url,method:req.method,headers:req.headers}, po=>{
    res.writeHead(po.statusCode,po.headers); po.pipe(res);});
  o.on('error',e=>{res.writeHead(502,{'content-type':'text/plain'});res.end('beacon: app not up\n'+logs.slice(-10).join('\n')+'\n');});
  req.pipe(o);
});
proxy.on('clientError',(e,sock)=>{ try{sock.end('HTTP/1.1 400 Bad Request\r\n\r\n');}catch(_){} });
proxy.on('upgrade',(req,socket,head)=>{
  if(!appUp){ try{socket.end();}catch(_){} return; }
  const o=http.request({host:'127.0.0.1',port:APPPORT,path:req.url,method:'GET',headers:{...req.headers}});
  o.end();
  o.on('upgrade',(por,sck,hd)=>{
    socket.write('HTTP/1.1 101 Switching Protocols\r\n'+Object.entries(por.headers).map(([k,v])=>k+': '+v).join('\r\n')+'\r\n\r\n'+hd);
    socket.pipe(sck); sck.pipe(socket);
    sck.on('error',()=>{});
    socket.on('error',()=>{});
  });
  o.on('response',po=>{ try{socket.end('HTTP/1.1 502 Bad Gateway\r\ncontent-length:0\r\n\r\n');}catch(_){} });
  o.on('error',()=>{ try{socket.end();}catch(_){} });
});
proxy.listen(PUBPORT,'0.0.0.0',()=>lg('proxy up on '+PUBPORT));
