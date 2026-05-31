import asyncio
import aiohttp
import sys
from datetime import datetime, timezone

class GemAsyncScanner:
    def __init__(self):
        self.headers = {"Accept": "application/json;version=20230302"}
        self.min_pump_percentage = 0.5
        self.min_ath_market_cap = 40000
        self.seen_pools = set()
        self.semaphore = asyncio.Semaphore(3)

    async def fetch_json(self, session, url):
        for attempt in range(5):
            try:
                async with self.semaphore:
                    async with session.get(url, headers=self.headers, timeout=20) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            await asyncio.sleep(5 * (attempt + 1))
                        else:
                            return None
            except Exception:
                await asyncio.sleep(2)
        return None

    async def get_trending_pools(self, session, page):
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page={page}"
        data = await self.fetch_json(session, url)
        return data.get('data', []) if data else []

    async def get_pool_candles_async(self, session, pool_address, pool_created_at_str):
        created_at = datetime.fromisoformat(pool_created_at_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        hours_old = (now - created_at).total_seconds() / 3600
        
        # 72 saat eşik: Bebek coinlerde 15dk, olgun coinlerde 1sa mum
        if hours_old < 72:
            url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/minute?aggregate=15&limit=1000"
        else:
            url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/hour?aggregate=1&limit=1000"
            
        data = await self.fetch_json(session, url)
        if data:
            ohlcv_data = data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])
            ohlcv_data.reverse()
            return ohlcv_data
        return []

    async def analyze_pool_async(self, session, pool_data):
        pool_attributes = pool_data.get('attributes', {})
        
        # YAŞ FİLTRESİ: 33 günden büyükse hiç uğraşma
        pool_created_at = pool_attributes.get('pool_created_at', datetime.now(timezone.utc).isoformat())
        created_at = datetime.fromisoformat(pool_created_at.replace("Z", "+00:00"))
        if (datetime.now(timezone.utc) - created_at).total_seconds() / 3600 > (33 * 24):
            return

        # ERKEN ELEME: Düşük hacimli çöp coinleri baştan ele
        if float(pool_attributes.get('volume_24h_usd', 0)) < 5000: return

        pool_address = pool_attributes.get('address')
        pool_name = pool_attributes.get('name')
        base_token_id = pool_data.get('relationships', {}).get('base_token', {}).get('data', {}).get('id', '')
        token_ca = base_token_id.split('_')[-1] if '_' in base_token_id else 'Bilinmiyor'

        candles = await self.get_pool_candles_async(session, pool_address, pool_created_at)
        if len(candles) < 20: return

        lows = [float(c[3]) for c in candles]
        highs = [float(c[2]) for c in candles]
        
        ath_price = max(highs)
        atl_price = min(lows)
        ath_index = highs.index(ath_price)
        
        if (ath_price * 1000000000) < self.min_ath_market_cap: return

        fark = ath_price - atl_price
        fib_0950 = ath_price - (fark * 0.950)
        fib_0618 = ath_price - (fark * 0.618)
        fib_0382 = ath_price - (fark * 0.382)

        post_ath_candles = candles[ath_index + 1:]
        if not post_ath_candles or any(float(c[3]) <= fib_0950 for c in post_ath_candles): return

        idx_0618 = next((i for i, c in enumerate(post_ath_candles) if float(c[3]) <= fib_0618), -1)
        if idx_0618 == -1: return

        sub_candles = post_ath_candles[idx_0618:]
        point4 = max([float(c[2]) for c in sub_candles])
        point5 = min([float(c[3]) for c in sub_candles])
        
        if point4 <= point5: return
        
        current_price = float(post_ath_candles[-1][4])
        ters_fib_0618 = point5 + (point4 - point5) * 0.618

        if current_price > ters_fib_0618 and point4 > fib_0382:
            print(f"\n🚀 [BİNGO! - ŞART 1] {pool_name} | CA: {token_ca}")
            print(f"   🎯 Ters Fibo 0.618 ({ters_fib_0618:.8f}) kırıldı!")
            print("-" * 65)

    async def scan_loop(self):
        toplam_sayfa = 10
        print(f"🚀 [GÜNCEL HİBRİT TARAYICI] (Yaş Sınırı: 33 Gün | Semaphore: 6)")
        
        async with aiohttp.ClientSession() as session:
            while True:
                self.seen_pools.clear()
                tasks = []
                for p in range(1, toplam_sayfa + 1): 
                    tasks.append(self.get_trending_pools(session, p))
                
                pages = await asyncio.gather(*tasks)
                pool_tasks = [self.analyze_pool_async(session, pool) for page in pages for pool in page]
                await asyncio.gather(*pool_tasks)

                print(f"\n✅ Döngü bitti. 3 dk mola...")
                await asyncio.sleep(180)

if __name__ == "__main__":
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(GemAsyncScanner().scan_loop())