// Serveur Los Lyros : sert le jeu + comptes joueurs (pseudo + code)
// Stockage : PostgreSQL si DATABASE_URL est défini, sinon fichier data/accounts.json
const http=require('http'),fs=require('fs'),path=require('path');
const PORT=process.env.PORT||10000,ROOT=__dirname;
let pg=null,pool=null;if(process.env.DATABASE_URL){try{pg=require('pg');pool=new pg.Pool({connectionString:process.env.DATABASE_URL,ssl:{rejectUnauthorized:false}});}catch(e){console.log('pg absent, stockage fichier');}}
const FILE=path.join(ROOT,'data','accounts.json');let mem={};
async function initStore(){if(pool){await pool.query('CREATE TABLE IF NOT EXISTS accounts (pseudo TEXT PRIMARY KEY, hash TEXT NOT NULL, save JSONB, updated TIMESTAMPTZ DEFAULT now())');}else{try{mem=JSON.parse(fs.readFileSync(FILE,'utf8'));}catch(e){mem={};}}}
async function getAcc(p){if(pool){const r=await pool.query('SELECT hash,save FROM accounts WHERE pseudo=$1',[p]);return r.rows[0]||null;}return mem[p]||null;}
async function setAcc(p,hash,save){if(pool){await pool.query('INSERT INTO accounts(pseudo,hash,save,updated) VALUES($1,$2,$3,now()) ON CONFLICT(pseudo) DO UPDATE SET save=EXCLUDED.save,updated=now()',[p,hash,save]);return;}mem[p]={hash,save};fs.mkdirSync(path.dirname(FILE),{recursive:true});fs.writeFileSync(FILE,JSON.stringify(mem));}
const TYPES={'.html':'text/html; charset=utf-8','.js':'application/javascript','.json':'application/json','.xml':'application/xml','.glb':'model/gltf-binary','.png':'image/png','.jpg':'image/jpeg','.css':'text/css'};
const hits={};function limited(ip){const now=Date.now();const h=hits[ip]=(hits[ip]||[]).filter(t=>now-t<60000);h.push(now);return h.length>60;}
function body(req){return new Promise((ok,ko)=>{let d='';req.on('data',c=>{d+=c;if(d.length>3e6){req.destroy();ko(new Error('trop gros'));}});req.on('end',()=>{try{ok(JSON.parse(d||'{}'));}catch(e){ko(e);}});});}
const send=(res,code,obj)=>{res.writeHead(code,{'Content-Type':'application/json'});res.end(JSON.stringify(obj));};
const okPseudo=p=>typeof p==='string'&&/^[\wÀ-ÿ\- ]{3,16}$/.test(p);
const server=http.createServer(async(req,res)=>{try{const url=decodeURIComponent(req.url.split('?')[0]);
  if(url==='/api/ping')return send(res,200,{ok:true,db:!!pool});
  if(url==='/api/login'&&req.method==='POST'){if(limited(req.socket.remoteAddress))return send(res,429,{error:'Trop de tentatives, attends une minute'});const b=await body(req);if(!okPseudo(b.pseudo)||typeof b.hash!=='string')return send(res,400,{error:'Pseudo invalide'});const p=b.pseudo.toLowerCase();const a=await getAcc(p);
    if(!a){await setAcc(p,b.hash,{});return send(res,200,{created:true,save:{}});}if(a.hash!==b.hash)return send(res,403,{error:'Code incorrect pour ce pseudo'});return send(res,200,{created:false,save:a.save||{}});}
  if(url==='/api/save'&&req.method==='POST'){const b=await body(req);if(!okPseudo(b.pseudo))return send(res,400,{error:'Pseudo invalide'});const p=b.pseudo.toLowerCase();const a=await getAcc(p);if(!a||a.hash!==b.hash)return send(res,403,{error:'Refusé'});await setAcc(p,a.hash,b.save||{});return send(res,200,{ok:true});}
  let f=path.normalize(path.join(ROOT,url==='/'?'index.html':url));if(!f.startsWith(ROOT)||f.includes(path.sep+'data'+path.sep)||f.endsWith('server.js')){res.writeHead(403);return res.end();}
  fs.stat(f,(e,st)=>{if(e||!st.isFile()){res.writeHead(404);return res.end('404');}res.writeHead(200,{'Content-Type':TYPES[path.extname(f)]||'application/octet-stream','Cache-Control':f.endsWith('.html')?'no-cache':'public, max-age=86400'});fs.createReadStream(f).pipe(res);});
}catch(e){console.error(e);send(res,500,{error:'Erreur serveur'});}});
server.listen(PORT,async()=>{await initStore();console.log('Los Lyros sur le port',PORT,pool?'(PostgreSQL)':'(fichier)');});

/* ---- Multijoueur temps réel (WebSocket) ---- */
let WSS=null;try{const {WebSocketServer}=require('ws');WSS=new WebSocketServer({server,path:'/ws',maxPayload:4096});}catch(e){console.log('ws absent : multijoueur désactivé');}
if(WSS){const players=new Map();let nid=1;const clean=t=>String(t||'').replace(/[<>]/g,'').slice(0,140);
  WSS.on('connection',ws=>{const id='p'+(nid++);const P={id,pseudo:'Joueur',look:{},st:null,chatT:0};players.set(ws,P);ws.send(JSON.stringify({t:'id',id}));
    ws.on('message',buf=>{let m;try{m=JSON.parse(buf);}catch(_){return;}
      if(m.t==='hello'){P.pseudo=clean(m.pseudo).slice(0,16)||'Joueur';P.look=(m.look&&typeof m.look==='object')?m.look:{};}
      else if(m.t==='st'){P.st={x:+m.x||0,y:+m.y||0,z:+m.z||0,yaw:+m.yaw||0,sp:+m.sp||0,car:m.car&&typeof m.car==='object'?{ty:clean(m.car.ty).slice(0,20),col:clean(m.car.col).slice(0,9),h:+m.car.h||0}:null,heli:!!m.heli,dead:!!m.dead};}
      else if(m.t==='chat'){const now=Date.now();if(now-P.chatT<800)return;P.chatT=now;const msg=JSON.stringify({t:'chat',from:P.pseudo,msg:clean(m.msg)});for(const c of WSS.clients)if(c.readyState===1)c.send(msg);}});
    ws.on('close',()=>players.delete(ws));});
  setInterval(()=>{const list=[];for(const P of players.values())if(P.st)list.push(Object.assign({id:P.id,pseudo:P.pseudo,look:P.look},P.st));const msg=JSON.stringify({t:'all',p:list});for(const c of WSS.clients)if(c.readyState===1)c.send(msg);},100);
  console.log('Multijoueur WebSocket actif sur /ws');}
