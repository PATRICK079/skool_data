"""
hud2_charts.py

last updated: 2025-07-12

todo:
- add unit tests
- add google style docs for doc generation
- decouple supabase logic
- define scope for this file
"""

# external
import datetime
from dateutil.relativedelta import relativedelta

# intern
from deps.database import connect_to_supabase


def parse_date(date_str):
    """
    Parse a date string into a datetime object
    """
    if date_str is None:
        return None
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")


def month_range(start, end):
    """
    Get the range of months between two dates
    """
    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append(current)
        current += relativedelta(months=1)
    return months


def get_member_mrr(member):
    """
    Get the MRR for a member
    """
    print(f"[MRR] For member {member.get('name', member.get('id'))}: {member['mrr']}")
    return member["mrr"]


def calculate_monthly_metrics(members, community_slug):
    """
    Calculate the monthly metrics for a community
    """
    print("\n📊 Starting monthly metrics calculation...")
    # Handle empty members list
    if not members:
        print("🚫 No members found. Skipping metrics calculation.")
        return []

    # Parse join/exit dates
    for m in members:
        m["joined_at_dt"] = parse_date(m["joined_at"])
        m["exited_at_dt"] = parse_date(m["exited_at"])

    # Find range
    min_join = min(m["joined_at_dt"] for m in members)
    max_exit = max(
        [m["exited_at_dt"] for m in members if m["exited_at_dt"]]
        + [datetime.datetime.today()]
    )
    print(
        f"📅 Calculating from {min_join.strftime('%Y-%m')} to {max_exit.strftime('%Y-%m')}"
    )
    months = month_range(min_join, max_exit)

    results = []
    for month_start in months:
        month_end = (month_start + relativedelta(months=1)) - datetime.timedelta(days=1)
        print(f"\n🗓️ Processing month: {month_start.strftime('%Y-%m')}")
        # Active at start
        active_start = [
            m
            for m in members
            if m["joined_at_dt"] <= month_start
            and (m["exited_at_dt"] is None or m["exited_at_dt"] > month_start)
        ]
        print(f"👥 Active members at start: {len(active_start)}")
        # Active at end
        active_end = [
            m
            for m in members
            if m["joined_at_dt"] <= month_end
            and (m["exited_at_dt"] is None or m["exited_at_dt"] > month_end)
        ]
        print(f"👥 Active members at end: {len(active_end)}")
        # New members
        new_members = [
            m
            for m in members
            if m["joined_at_dt"].year == month_start.year
            and m["joined_at_dt"].month == month_start.month
        ]
        print(f"🆕 New members this month: {len(new_members)}")
        # Churned members
        churned_members = [
            m
            for m in members
            if m["exited_at_dt"]
            and m["exited_at_dt"].year == month_start.year
            and m["exited_at_dt"].month == month_start.month
        ]
        print(f"💔 Churned members this month: {len(churned_members)}")
        # MRR
        mrr = sum(get_member_mrr(m) for m in active_end)
        print(f"💵 MRR for {month_start.strftime('%Y-%m')}: {mrr:.2f}")
        # Churn count and rate
        churn_count = len(churned_members)
        churn_rate = churn_count / len(active_start) if len(active_start) > 0 else 0
        print(f"📉 Churn rate: {churn_rate:.4f}")
        # ARPU (only paying users)
        paying_active_start = [m for m in active_start if get_member_mrr(m) > 0]
        arpu = mrr / len(paying_active_start) if len(paying_active_start) > 0 else 0
        print(f"💰 ARPU (paying): {arpu:.2f}")
        # LTV (industry standard: ARPU / churn rate)
        ltv = arpu / churn_rate if churn_rate > 0 else None
        print(f"🏆 LTV: {'{:.2f}'.format(ltv) if ltv is not None else 'N/A'}")
        # Acquisition rate
        acq_rate = len(new_members) / len(active_start) if len(active_start) > 0 else 0
        print(f"📈 Acquisition rate: {acq_rate:.4f}")
        results.append(
            {
                "community_slug": community_slug,
                "month": month_start.strftime("%Y-%m"),
                "mrr": mrr,
                "ltv": ltv,
                "new_members": len(new_members),
                "acquisition_rate": acq_rate,
                "churned_members": churn_count,
                "churn_rate": churn_rate,
                "price": sum(m.get("price", 0) for m in active_end),
            }
        )
    print("\n✅ Finished calculating all monthly metrics!")
    return results


def upsert_hud2_charts(supabase, metrics):
    print("\n🚀 Starting delete-then-insert to hud2_charts table...")
    try:
        # Collect all unique (community_slug, month) pairs
        unique_keys = set((row["community_slug"], row["month"]) for row in metrics)
        for community_slug, month in unique_keys:
            print(
                f"🗑️ Deleting existing rows for community_slug='{community_slug}', month='{month}'..."
            )
            supabase.table("hud2_charts").delete().eq(
                "community_slug", community_slug
            ).eq("month", month).execute()
        print(f"➕ Inserting {len(metrics)} new rows...")
        response = supabase.table("hud2_charts").insert(metrics).execute()
        print(
            f"✅ Insert complete! Rows affected: {getattr(response, 'count', 'unknown')} 🟢"
        )
        print(f"📝 Supabase response: {response}")
    except Exception as e:
        print(f"❌ Error during delete-then-insert: {e}")


if __name__ == "__main__":
    pass
