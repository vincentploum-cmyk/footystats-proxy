const express=require("express"),cors=require("cors"),fetch=require("node-fetch"),app=express();
app.use(cors({origin:"*",methods:["GET","OPTIONS"],allowedHeaders:["Content-Type","Authorization"]}));
app.options("*",cors());
const KEY="437fa5361a693ad65c0c97d75f55042da3529532df53b57d34fe28f89789c0e7",BASE="https://api.football-data-api.com";
app.get("/",(req,res)=>res.json({status:"ok"}));
app.get("/api/*",async(req,res)=>{
  try{
    const path=req.path.replace("/api","");
    const qs=new URLSearchParams({...req.query,key:KEY}).toString();
    const url=`${BASE}${path}?${qs}`;
    console.log("Fetching:",url);
    const r=await fetch(url);
    const data=await r.json();
    res.header("Access-Control-Allow-Origin","*");
    res.json(data);
  }catch(e){res.status(500).json({error:e.message})}
});
app.listen(process.env.PORT||3001,()=>console.log("Proxy running"));
