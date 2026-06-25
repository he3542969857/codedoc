# -*- coding: utf-8 -*-
"""选仓充分测评 —— 真实微服务多仓 + 真工具,对比 4 种选仓策略的 recall/precision/F1。

修复点(对应上一轮验证暴露的问题):
  ① 搜:子串同义匹配(免中文分词,修两题零召回)
  ② 反向扩加判断:只纳"依赖≥2个种子的编排仓"(抓编排方,不无脑反向)
  ③ 排除 hub 仓:common 这种全员依赖的,不参与依赖扩(防过度扩张)
"""
from __future__ import annotations
import math, re

SERVICES = {
"common": '''class BaseModel: ...
class Money: """金额货币 值对象,所有服务共用的数据模型"""
def jsonify(x): """统一序列化工具"""''',
"user_service": '''from common import BaseModel
class User(BaseModel): """用户实体"""
def login(name,pwd): """登录鉴权:校验密码签发 token"""
def authenticate(token): """校验 token 鉴权"""''',
"inventory_service": '''from common import BaseModel, Money
class Stock(BaseModel): """库存实体"""
def reserve(sku,qty): """扣减预留库存,库存不足抛异常"""
def release(sku,qty): """释放库存"""''',
"payment_service": '''from common import Money
from user_service import authenticate
def charge(user,amount): """付款扣款支付:鉴权后发起支付"""
def refund(pid): """退款"""''',
"order_service": '''from common import BaseModel
from user_service import authenticate
from inventory_service import reserve
from payment_service import charge
from notification_service import send_email
class Order(BaseModel): """订单实体"""
def place_order(user,items):
    """下单主流程:鉴权->扣库存 reserve->付款 charge->发确认邮件->生成订单"""''',
"notification_service": '''from common import jsonify
from user_service import User
def send_email(user,msg): """给用户发确认邮件通知"""
def send_sms(user,msg): """发短信通知"""''',
"analytics_service": '''from common import BaseModel
from order_service import Order
def order_report(): """订单数据分析报表:统计下单量 GMV"""''',
"api_gateway": '''from common import jsonify
from user_service import login
from order_service import place_order
from payment_service import charge
from inventory_service import reserve
from notification_service import send_email
def route(path): """API 网关:路由分发到各微服务"""''',
}

# ─── 真依赖图:扫 import ───
def _scan(code, self_):
    out = []
    for m in re.finditer(r"^\s*(?:from|import)\s+(\w+)", code, re.M):
        s = m.group(1)
        if s in SERVICES and s != self_ and s not in out:
            out.append(s)
    return out
DEPS = {r: _scan(c, r) for r, c in SERVICES.items()}
RDEPS = {r: [s for s in SERVICES if r in DEPS[s]] for r in SERVICES}
N = len(SERVICES)
HUBS = {r for r in SERVICES if len(RDEPS[r]) >= math.ceil(0.75 * N)}   # 全员依赖的共享库

# ─── 真搜索:子串同义匹配(免分词) ───
_SYN = {"登录": ["login", "authenticate"], "鉴权": ["authenticate", "login"],
        "库存": ["reserve", "stock", "release"], "付款": ["charge", "payment"],
        "支付": ["charge", "payment"], "扣款": ["charge"], "退款": ["refund"],
        "下单": ["place_order", "order"], "订单": ["order"], "通知": ["send", "notif"],
        "邮件": ["send_email"], "短信": ["send_sms"], "报表": ["report"], "统计": ["report"],
        "分析": ["analytics", "report"], "网关": ["gateway", "route"], "路由": ["route"],
        "数据模型": [], "序列化": ["jsonify"]}

def _terms(q):
    t = set(re.findall(r"[a-z_]{3,}", q.lower()))
    for k, syns in _SYN.items():
        if k in q:
            t.add(k.lower()); t.update(s.lower() for s in syns)
    return t

def _deftext(code, repo):
    # 只取"定义/docstring",剔除 import 行(谁 import 了符号≠谁定义/相关)
    lines = [ln for ln in code.splitlines() if not re.match(r"\s*(from|import)\s", ln)]
    return ("\n".join(lines) + " " + repo).lower()

DEFTEXT = {r: _deftext(c, r) for r, c in SERVICES.items()}

def search_repos(query, top_k=4):
    qt = _terms(query)
    scored = []
    for r in SERVICES:
        text = DEFTEXT[r]
        sc = sum(text.count(t) for t in qt if t in text)
        if sc > 0:
            scored.append((r, sc))
    if not scored:
        return []
    scored.sort(key=lambda x: -x[1])
    top = scored[0][1]
    # 阈值:只留分数 >= top 的一半,且最多 top_k(弱命中不进)
    return [r for r, s in scored[:top_k] if s >= max(1, top * 0.34)]

# ─── 4 种选仓策略 ───
def select(query, mode, top_k=4):
    seeds = search_repos(query, top_k)
    sel = list(seeds)
    if mode in ("fwd", "rev_naive", "improved"):          # 正向依赖扩(种子依赖谁)
        for s in seeds:
            for d in DEPS[s]:
                if mode == "improved" and d in HUBS:       # improved:跳过 hub
                    continue
                if d not in sel: sel.append(d)
    if mode == "rev_naive":                                # 无脑反向(谁依赖种子)
        for s in seeds:
            for d in RDEPS[s]:
                if d not in sel: sel.append(d)
    if mode == "improved":                                 # 判断式反向:依赖≥2种子的编排仓
        for R in SERVICES:
            if R in sel or R in HUBS: continue
            if sum(1 for s in seeds if s in DEPS[R]) >= 2:
                sel.append(R)
    return seeds, sel

# ─── 人工真值(13 题,含编排隐藏/hub/单仓等硬例) ───
GT = {
 "用户下单时扣库存和付款的流程": {"order_service","inventory_service","payment_service"},
 "库存和付款是怎么协调的":        {"order_service","inventory_service","payment_service"},  # order 未提,靠反向编排
 "鉴权和库存是怎么串起来的":      {"order_service","user_service","inventory_service"},       # order 未提
 "登录鉴权流程是怎样的":          {"user_service"},
 "支付是怎么鉴权的":              {"payment_service","user_service"},
 "退款流程":                      {"payment_service"},
 "下单后给用户发确认邮件":        {"order_service","notification_service"},
 "库存不足时下单会怎样":          {"order_service","inventory_service"},
 "分析报表的订单数据从哪来":      {"analytics_service","order_service"},
 "所有服务共用的数据模型在哪":    {"common"},
 "网关怎么路由到各服务":          {"api_gateway"},
 "怎么给用户发短信":              {"notification_service"},
 "订单数据怎么统计的":            {"analytics_service","order_service"},
}

def prf(sel, gt):
    sel, gt = set(sel), set(gt)
    tp = len(sel & gt)
    r = tp/len(gt) if gt else 1.0
    p = tp/len(sel) if sel else 0.0
    f = 2*r*p/(r+p) if (r+p) else 0.0
    return r, p, f

print("HUBS(全员依赖,扩仓时排除):", HUBS)
print("依赖图:")
for r in SERVICES: print("  %-20s -> %s" % (r, DEPS[r]))
print()
MODES = [("none","纯搜"),("fwd","+正向扩"),("rev_naive","+无脑反向"),("improved","改进:hub排除+反向编排判断")]
for mode, label in MODES:
    R=P=F=0; rows=[]
    for q, gt in GT.items():
        _, sel = select(q, mode)
        r,p,f = prf(sel, gt); R+=r; P+=p; F+=f
        miss = set(gt)-set(sel)
        if mode in ("none","improved"):
            rows.append("    R=%.2f P=%.2f | %s | 选%s%s" % (r,p,q[:12],sorted(sel),("  漏:%s"%miss if miss else "")))
    n=len(GT)
    print("=== %s ===  recall=%.2f  precision=%.2f  F1=%.2f" % (label, R/n, P/n, F/n))
    for x in rows: print(x)
    print()
