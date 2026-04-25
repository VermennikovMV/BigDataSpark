"""
Генерация диаграмм схемы БД:
  1. star_schema.png  — модель «звезда» в PostgreSQL
  2. clickhouse_marts.png — витрины ClickHouse
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch

# ─── Helpers ────────────────────────────────────────────────────────────────

def draw_table(ax, x, y, width, title, fields, header_color, body_color,
               field_colors=None):
    """Draw an ER table box; returns (x_center, y_center, total_height)."""
    ROW_H   = 0.38
    HEAD_H  = 0.52
    PAD     = 0.12
    n       = len(fields)
    height  = HEAD_H + n * ROW_H + PAD

    # Shadow
    shadow = mpatches.FancyBboxPatch(
        (x + 0.04, y - height - 0.04), width, height,
        boxstyle="round,pad=0.04", linewidth=0,
        facecolor="#cccccc", zorder=1)
    ax.add_patch(shadow)

    # Body
    body = mpatches.FancyBboxPatch(
        (x, y - height), width, height,
        boxstyle="round,pad=0.04", linewidth=1.2,
        edgecolor="#555555", facecolor=body_color, zorder=2)
    ax.add_patch(body)

    # Header
    head = mpatches.FancyBboxPatch(
        (x, y - HEAD_H), width, HEAD_H,
        boxstyle="round,pad=0.04", linewidth=0,
        facecolor=header_color, zorder=3)
    ax.add_patch(head)

    # Header text
    ax.text(x + width / 2, y - HEAD_H / 2, title,
            ha="center", va="center", fontsize=9.5,
            fontweight="bold", color="white", zorder=4)

    # Separator line
    ax.plot([x + 0.05, x + width - 0.05], [y - HEAD_H, y - HEAD_H],
            color="#aaaaaa", linewidth=0.6, zorder=4)

    # Fields
    for i, field in enumerate(fields):
        fy = y - HEAD_H - (i + 0.5) * ROW_H - PAD / 2
        fc = field_colors[i] if field_colors else "#222222"
        size = 7.8
        ax.text(x + 0.15, fy, field,
                ha="left", va="center", fontsize=size,
                color=fc, fontfamily="monospace", zorder=4)

    cx = x + width / 2
    cy = y - height / 2
    return cx, cy, height, x, y


def connect(ax, x1, y1, x2, y2, color="#666666"):
    """Draw a dashed arrow from (x1,y1) to (x2,y2)."""
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(
                    arrowstyle="-|>",
                    color=color,
                    lw=1.2,
                    linestyle="dashed",
                    connectionstyle="arc3,rad=0.0",
                    mutation_scale=10,
                ), zorder=5)


# ════════════════════════════════════════════════════════════════════════════
#  1. Star Schema (PostgreSQL)
# ════════════════════════════════════════════════════════════════════════════

fig, ax = plt.subplots(figsize=(18, 13))
ax.set_xlim(0, 18)
ax.set_ylim(-13, 0.5)
ax.axis("off")
ax.set_facecolor("#f9f9f9")
fig.patch.set_facecolor("#f9f9f9")

fig.suptitle("Модель «звезда» — PostgreSQL (БД bigdata)",
             fontsize=14, fontweight="bold", y=0.99)

W  = 3.6   # table width
Wf = 4.4   # fact width

# ── fact_sales (centre) ──────────────────────────────────────────────────
fact_fields = [
    "[PK] sale_id      BIGINT",
    "[FK] customer_id  BIGINT",
    "[FK] seller_id    BIGINT",
    "[FK] product_id   BIGINT",
    "[FK] store_id     BIGINT",
    "[FK] supplier_id  BIGINT",
    "[FK] date_id      INT",
    "     sale_date    DATE",
    "     quantity     INT",
    "     total_price  DECIMAL(18,2)",
]
fact_fc = (
    ["#b8860b"] +
    ["#6a0dad"] * 6 +
    ["#222222"] * 3
)
fx, fy = 6.8, -1.5
draw_table(ax, fx, fy, Wf, "fact_sales", fact_fields,
           "#c8860b", "#fffbe6", fact_fc)

# ── dim_customer (top-left) ──────────────────────────────────────────────
dc_fields = [
    "[PK] customer_id  BIGINT",
    "     first_name   VARCHAR",
    "     last_name    VARCHAR",
    "     age          INT",
    "     email        VARCHAR",
    "     country      VARCHAR",
    "     postal_code  VARCHAR",
    "     pet_type     VARCHAR",
    "     pet_name     VARCHAR",
    "     pet_breed    VARCHAR",
]
dc_fc = ["#1a5276"] + ["#222222"] * 9
draw_table(ax, 0.3, -0.3, W, "dim_customer", dc_fields,
           "#2471a3", "#eaf4fb", dc_fc)
connect(ax, 0.3 + W, -0.3 - 0.26, fx, fy - 0.26 - 0.38, "#2471a3")

# ── dim_seller (top-right) ───────────────────────────────────────────────
ds_fields = [
    "[PK] seller_id    BIGINT",
    "     first_name   VARCHAR",
    "     last_name    VARCHAR",
    "     email        VARCHAR",
    "     country      VARCHAR",
    "     postal_code  VARCHAR",
]
ds_fc = ["#1a5276"] + ["#222222"] * 5
draw_table(ax, 14.1, -0.3, W, "dim_seller", ds_fields,
           "#2471a3", "#eaf4fb", ds_fc)
connect(ax, 14.1, -0.3 - 0.26, fx + Wf, fy - 0.26 - 0.38 * 2, "#2471a3")

# ── dim_product (right) ──────────────────────────────────────────────────
dp_fields = [
    "[PK] product_id   BIGINT",
    "     name         VARCHAR",
    "     category     VARCHAR",
    "     price        DECIMAL(18,2)",
    "     weight       DOUBLE",
    "     color        VARCHAR",
    "     size         VARCHAR",
    "     brand        VARCHAR",
    "     material     VARCHAR",
    "     description  TEXT",
    "     rating       DOUBLE",
    "     reviews      INT",
    "     release_date DATE",
    "     expiry_date  DATE",
    "     pet_category VARCHAR",
]
dp_fc = ["#155724"] + ["#222222"] * 14
draw_table(ax, 14.1, -4.0, W, "dim_product", dp_fields,
           "#28a745", "#eafaf1", dp_fc)
connect(ax, 14.1, -4.0 - 0.26, fx + Wf, fy - 0.26 - 0.38 * 3, "#28a745")

# ── dim_store (bottom-right) ─────────────────────────────────────────────
dstore_fields = [
    "[PK] store_id     BIGINT",
    "     name         VARCHAR",
    "     location     VARCHAR",
    "     city         VARCHAR",
    "     state        VARCHAR",
    "     country      VARCHAR",
    "     phone        VARCHAR",
    "     email        VARCHAR",
]
dstore_fc = ["#4a235a"] + ["#222222"] * 7
draw_table(ax, 14.1, -9.0, W, "dim_store", dstore_fields,
           "#8e44ad", "#fdf2f8", dstore_fc)
connect(ax, 14.1, -9.0 - 0.26, fx + Wf, fy - 0.26 - 0.38 * 4, "#8e44ad")

# ── dim_supplier (bottom-left) ───────────────────────────────────────────
dsup_fields = [
    "[PK] supplier_id  BIGINT",
    "     name         VARCHAR",
    "     contact      VARCHAR",
    "     email        VARCHAR",
    "     phone        VARCHAR",
    "     address      VARCHAR",
    "     city         VARCHAR",
    "     country      VARCHAR",
]
dsup_fc = ["#784212"] + ["#222222"] * 7
draw_table(ax, 0.3, -9.0, W, "dim_supplier", dsup_fields,
           "#e67e22", "#fef5e7", dsup_fc)
connect(ax, 0.3 + W, -9.0 - 0.26, fx, fy - 0.26 - 0.38 * 5, "#e67e22")

# ── dim_date (bottom-centre-left) ────────────────────────────────────────
dd_fields = [
    "[PK] date_id   INT",
    "     date      DATE",
    "     year      INT",
    "     quarter   INT",
    "     month     INT",
    "     day       INT",
]
dd_fc = ["#922b21"] + ["#222222"] * 5
draw_table(ax, 0.3, -4.5, W, "dim_date", dd_fields,
           "#c0392b", "#fdedec", dd_fc)
connect(ax, 0.3 + W, -4.5 - 0.26, fx, fy - 0.26 - 0.38 * 6, "#c0392b")

# Legend
legend_x, legend_y = 6.8, -11.2
ax.text(legend_x, legend_y, "Обозначения:", fontsize=9, fontweight="bold")
ax.annotate("", xy=(legend_x + 1.4, legend_y - 0.4),
            xytext=(legend_x + 0.1, legend_y - 0.4),
            arrowprops=dict(arrowstyle="-|>", color="#666", lw=1.2,
                            linestyle="dashed", mutation_scale=10))
ax.text(legend_x + 1.55, legend_y - 0.4,
        "FK-связь (dim → fact_sales)", fontsize=8.5, va="center")
ax.text(legend_x, legend_y - 0.8, "[PK] - первичный ключ", fontsize=8.5)
ax.text(legend_x, legend_y - 1.1, "[FK] - внешний ключ в fact_sales", fontsize=8.5)

plt.tight_layout()
plt.savefig("star_schema.png", dpi=180, bbox_inches="tight",
            facecolor="#f9f9f9")
plt.close()
print("star_schema.png saved")


# ════════════════════════════════════════════════════════════════════════════
#  2. ClickHouse Data Marts
# ════════════════════════════════════════════════════════════════════════════

fig2, ax2 = plt.subplots(figsize=(18, 11))
ax2.set_xlim(0, 18)
ax2.set_ylim(-11, 0.5)
ax2.axis("off")
ax2.set_facecolor("#f9f9f9")
fig2.patch.set_facecolor("#f9f9f9")
fig2.suptitle("Аналитические витрины — ClickHouse (БД reports)\n"
              "ENGINE = MergeTree() для всех таблиц",
              fontsize=13, fontweight="bold", y=0.99)

TW = 5.4   # mart width

# ── product_sales_mart ───────────────────────────────────────────────────
pm_fields = [
    "[PK] product_id      Int64",
    "     product_name    Nullable(String)",
    "     category        Nullable(String)",
    "     total_quantity  Int64",
    "     total_revenue   Decimal(18,2)",
    "     avg_rating      Nullable(Float64)",
    "     review_count    Nullable(Int64)",
    "     order_count     Int64",
]
pm_fc = ["#155724"] + ["#222222"] * 7
draw_table(ax2, 0.3, -0.3, TW, "product_sales_mart", pm_fields,
           "#28a745", "#eafaf1", pm_fc)

# ── customer_sales_mart ──────────────────────────────────────────────────
cm_fields = [
    "[PK] customer_id   Int64",
    "     customer_name Nullable(String)",
    "     country       Nullable(String)",
    "     total_spent   Decimal(18,2)",
    "     order_count   Int64",
    "     avg_check     Decimal(18,2)",
]
cm_fc = ["#1a5276"] + ["#222222"] * 5
draw_table(ax2, 6.3, -0.3, TW, "customer_sales_mart", cm_fields,
           "#2471a3", "#eaf4fb", cm_fc)

# ── time_sales_mart ──────────────────────────────────────────────────────
tm_fields = [
    "[PK] year           Int32",
    "[PK] month          Int32",
    "     total_revenue  Decimal(18,2)",
    "     order_count    Int64",
    "     avg_order_size Decimal(18,2)",
]
tm_fc = ["#922b21", "#922b21"] + ["#222222"] * 3
draw_table(ax2, 12.3, -0.3, TW, "time_sales_mart", tm_fields,
           "#c0392b", "#fdedec", tm_fc)

# ── store_sales_mart ─────────────────────────────────────────────────────
sm_fields = [
    "[PK] store_id       Int64",
    "     store_name     Nullable(String)",
    "     store_city     Nullable(String)",
    "     store_country  Nullable(String)",
    "     total_revenue  Decimal(18,2)",
    "     order_count    Int64",
    "     avg_check      Decimal(18,2)",
]
sm_fc = ["#4a235a"] + ["#222222"] * 6
draw_table(ax2, 0.3, -6.0, TW, "store_sales_mart", sm_fields,
           "#8e44ad", "#fdf2f8", sm_fc)

# ── supplier_sales_mart ──────────────────────────────────────────────────
supm_fields = [
    "[PK] supplier_id      Int64",
    "     supplier_name    Nullable(String)",
    "     supplier_country Nullable(String)",
    "     total_revenue    Decimal(18,2)",
    "     avg_product_price Decimal(18,2)",
    "     order_count      Int64",
]
supm_fc = ["#784212"] + ["#222222"] * 5
draw_table(ax2, 6.3, -6.0, TW, "supplier_sales_mart", supm_fields,
           "#e67e22", "#fef5e7", supm_fc)

# ── quality_mart ─────────────────────────────────────────────────────────
qm_fields = [
    "[PK] product_id      Int64",
    "     product_name    Nullable(String)",
    "     rating          Nullable(Float64)",
    "     review_count    Nullable(Int64)",
    "     total_quantity  Int64",
    "     total_revenue   Decimal(18,2)",
]
qm_fc = ["#b8860b"] + ["#222222"] * 5
draw_table(ax2, 12.3, -6.0, TW, "quality_mart", qm_fields,
           "#c8860b", "#fffbe6", qm_fc)

# Source label
ax2.text(9, -10.0, "Источник данных: PostgreSQL bigdata (fact_sales + dim_*)\n"
         "Трансформация: Apache Spark 3.5.3 (JDBC → groupBy/agg → join)",
         ha="center", va="center", fontsize=9.5,
         bbox=dict(boxstyle="round,pad=0.5", facecolor="#d5e8d4",
                   edgecolor="#82b366", linewidth=1.2))

plt.tight_layout()
plt.savefig("clickhouse_marts.png", dpi=180, bbox_inches="tight",
            facecolor="#f9f9f9")
plt.close()
print("clickhouse_marts.png saved")
