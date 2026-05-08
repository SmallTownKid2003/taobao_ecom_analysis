import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# ====================== 1. 连接 MySQL ======================
engine = create_engine('mysql+pymysql://root:123456@localhost:3306/ecom_taobao')

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


# ==============================================
# 图1：每日支付订单量趋势图
# ==============================================
sql1 = '''
SELECT DATE(dt) AS d,
       COUNT(CASE WHEN behavior='pay' THEN 1 END) AS pay_cnt
FROM userbehavior
GROUP BY d
ORDER BY d;
'''
df1 = pd.read_sql(sql1, engine)

plt.figure(figsize=(14,5))
plt.plot(df1['d'], df1['pay_cnt'], marker='o', color='#ff6700', linewidth=2)
plt.title('淘宝每日支付订单量趋势', fontsize=14)
plt.xticks(rotation=45)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig('daily_pay_trend.png', dpi=300)  # 自动保存图片
plt.show()


# ==============================================
# 图2：热销类目TOP10（支付次数）
# ==============================================
sql2 = '''
SELECT category_id,
       COUNT(CASE WHEN behavior='pay' THEN 1 END) AS pay_cnt
FROM userbehavior
GROUP BY category_id
ORDER BY pay_cnt DESC
LIMIT 10;
'''
df2 = pd.read_sql(sql2, engine)

plt.figure(figsize=(12,5))
plt.bar(df2['category_id'].astype(str), df2['pay_cnt'], color='#1890ff')
plt.title('淘宝热销类目 TOP10', fontsize=14)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('category_top10.png', dpi=300)
plt.show()


# ==============================================
# 图3：24小时访问流量分布
# ==============================================
sql3 = '''
SELECT HOUR(dt) AS hour,
       COUNT(DISTINCT user_id) AS uv
FROM userbehavior
GROUP BY hour
ORDER BY hour;
'''
df3 = pd.read_sql(sql3, engine)

plt.figure(figsize=(12,5))
plt.plot(df3['hour'], df3['uv'], marker='o', color='#52c41a', linewidth=2)
plt.xticks(range(0,24))
plt.title('淘宝24小时用户访问量分布', fontsize=14)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig('hourly_uv.png', dpi=300)
plt.show()