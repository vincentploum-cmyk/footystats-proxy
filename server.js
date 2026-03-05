const express=require("express"),cors=require("cors"),fetch=require("node-fetch"),app=express();
app.use(cors());
const KEY="437fa5361a693ad65c0c97d75f55042da3529532df53b57d34fe28f89789c0e7",BASE="https://api.football-data-api.com";
app.get("/",(req,res)=>res.json({status:"ok"}));
app.get("/api/*",async(req,res)=>{try{const path=req.path.replace("/api",""),qs=new URLSearchParams({...req.query,key:KEY}).toString(),url=`${BASE}${path}?${qs}`,r=await fetch(url),data=await r.json();res.json(data)}catch(e){res.status(500).json({error:e.message})}});
app.listen(process.env.PORT||3001,()=>console.log("Proxy running"));
