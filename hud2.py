"""
hud2.py

last updated: 2025-07-12

# How we get each metric:
#1. MRR: sum of all mrr fields
#2. MRR (no annual): sum of all mrr fields where plan_type is not annual
#3. Retained Revenue (past 30 days): sum of 'mrr' for members who were active 30 days ago AND are still active today.
#4. Avg. LTV: ARPU / Churn Rate, where ARPU = Total MRR / Total active paying members (members with price > 0), and Churn Rate is the 30-day churn rate (as a decimal, not percent). If churn rate is zero, LTV is infinite.
#5 Avg. LTV (just monthly): same as above but only for monthly members.
#6: Avg lifespan (just monthly): average of all the 'joined_at' and 'exited_at' fields for monthly members who have left.
#7. Growth rate:  (Members active today - Members active 30 days ago) / Members active 30 days ago.")
#8. Acquisition Rate:  New members (30d) / Members active 30 days ago.")
#9. Churn rate: (Number of members who left in the last 30 days) / (Number of members who were active 30 days ago) * 100%.")
#10: Total members: total members in active tab.
#11. Paid members: total members in active tab where price > 0.
#12. New members (30d): count of members whose 'joined_at' date is within the last 30 days.
#13. Churned members (30d): count of members whose 'exited_at' date is within the last 30 days.
#14. Member ceiling: new members in last 30d / churn rate
#15. MRR ceiling:  Member Ceiling * Average price per member
#16. Days to growth ceiling:  (log((Member Ceiling - Current Members) / Member Ceiling) / log(1 - Churn Rate)) * 30
#17. Months to growth ceiling: Days to Growth Ceiling / 30

todo
- add google style docs for doc generation
- add unit tests
- decouple supabase logic
- define scope for this file
- decouple large functions into smaller functions
- database modeling for hud2
- convert this to elt where other functions pull everything,
- this function calculates stuff for mart
"""

# external
from typing import Dict
import math
from datetime import datetime, timedelta, timezone

# internal
from deps.members import (
    is_admin_in_group,
    add_any_new_members_from_scraped_to_crm,
    get_members_on_page,
    get_member_details,
    get_all_members_from_db_crm,
    get_all_members_from_db_scraped,
    add_member_to_db,
    add_members_from_api_to_db,
    sync_churned_status,
    get_all_members_for_community,
    get_all_community_members_and_save,
    billing_products_to_dict,
    count_new_members_since_efficient,
)
from deps.hud2_charts import (
    parse_date,
    month_range,
    get_member_mrr,
    calculate_monthly_metrics,
    upsert_hud2_charts,
)


def update_hud2(community_slug, build_id, auth_token, supabase):
    active_members, churned_members, billing_products = (
        get_all_community_members_and_save(
            community_slug,
            build_id,
            auth_token=auth_token,
            return_billing_products=True,
        )
    )
    cancelling_members = get_all_members_for_community(
        community_slug, build_id, auth_token, tab="cancelling"
    )
    formatted_billing_products = billing_products_to_dict(billing_products)

    ## Format the members
    # NOTE: someone who is "churning/declining" is still active according to skool.
    formatted_active_members = [
        convert_member_to_hud2(
            m,
            community_slug,
            force_active=True,
            billing_products=formatted_billing_products,
        )
        for m in active_members
    ]
    formatted_churned_members = [
        convert_member_to_hud2(
            m,
            community_slug,
            force_active=False,
            billing_products=formatted_billing_products,
        )
        for m in churned_members
    ]
    formatted_cancelling_members = [
        convert_member_to_hud2(
            m,
            community_slug,
            force_active=False,
            billing_products=formatted_billing_products,
        )
        for m in cancelling_members
    ]
    #### for each of these cancelling, set the 'active_or_cancelling' to 'cancelling'
    for m in formatted_cancelling_members:
        m["active_or_cancelling"] = "cancelling"

    formatted_members = formatted_active_members + formatted_churned_members
    # NOTE: we dont inclide cancelling because they are duplicates of the active members.

    ### Dashboard Section -------------------------
    # --- HUD2 Community Stats Summary ---
    print("\n" + "=" * 40)
    print("HUD2 COMMUNITY STATS SUMMARY")
    print("=" * 40 + "\n")

    total_rev_ytd_recurring_only = get_total_rev_ytd_recurring_only(
        [*formatted_active_members, *formatted_churned_members]
    )
    print(f"  Total MRR YTD:           ${total_rev_ytd_recurring_only/100:,.2f}")

    # --- MRR (Monthly Recurring Revenue) ---
    print("MRR (Monthly Recurring Revenue):")
    print("  Formula: Total MRR = sum of the 'mrr' value for all active members.")
    print(
        "  Formula (no annual): MRR (no annual) = sum of the 'mrr' value for all active members who are not on an annual plan."
    )
    mrr = calculate_mrr(formatted_members)
    mrr_no_annual = calculate_mrr_no_annual(formatted_members)
    print(f"  Total MRR:           ${mrr/100:,.2f}")
    print(f"  MRR (no annual):     ${mrr_no_annual/100:,.2f}\n")

    # --- Member Counts ---
    print("Member Counts:")
    print(
        "  Formula: Total active members = count of members where 'is_active' is True."
    )
    print(
        "  Formula: Total active paying members = count of members where 'is_active' is True and 'price' > 0."
    )
    total_active_members = len([m for m in formatted_members if m.get("is_active")])
    total_active_paying_members = len(
        [m for m in formatted_members if m.get("is_active") and m.get("price", 0) > 0]
    )
    print(f"  Total active members:         {total_active_members}")
    print(f"  Total active paying members:  {total_active_paying_members}\n")

    # --- Churn (30d) ---
    print("Churn (Past 30 Days):")
    print(
        "  Formula: Churn rate = (Number of members who left in the last 30 days) / (Number of members who were active 30 days ago) * 100%."
    )
    print("  Formula (no annual): Same as above, but only for non-annual members.")
    churn_pct = churn_count_past_30(formatted_members, debug=True)
    churn_pct_no_annual = churn_count_past_30(
        formatted_members, no_annual=True, debug=True
    )
    print(f"  Churn rate:           {churn_pct:.2f}%")
    print(f"  Churn rate (no annual): {churn_pct_no_annual:.2f}%\n")

    # --- Average Lifespan (no annual) ---
    print("Average Lifespan (No Annual):")
    print(
        "  Formula: Average lifespan = Average number of days between 'joined_at' and 'exited_at' for non-annual members who have left."
    )
    avg_lifespan_no_annual = average_lifespan_no_annual(formatted_members)
    print(f"  {avg_lifespan_no_annual:.1f} days\n")

    # --- Monthly Metrics (Past 30 Days) ---
    print("Monthly Metrics (Past 30 Days):")
    print(
        "  Formula: New members (30d) = count of members whose 'joined_at' date is within the last 30 days."
    )
    print(
        "  Formula: Churned members (30d) = count of members whose 'exited_at' date is within the last 30 days."
    )
    print(
        "  Formula: Acquisition rate (30d) = New members (30d) / Members active 30 days ago."
    )
    print(
        "  Formula: Churn rate (30d) = Churned members (30d) / Members active 30 days ago."
    )
    print(
        "  Formula: Retained revenue (30d) = sum of 'mrr' for members who were active 30 days ago AND are still active today."
    )
    print(
        "  Formula: Growth rate (30d) = (Members active today - Members active 30 days ago) / Members active 30 days ago."
    )

    now = datetime.today().date()
    days_ago_30 = now - timedelta(days=30)
    days_ago_60 = now - timedelta(days=60)

    # Parse join/exit dates for all members
    for m in formatted_members:
        m["joined_at_dt"] = (
            datetime.strptime(m["joined_at"], "%Y-%m-%d").date()
            if m["joined_at"]
            else None
        )
        m["exited_at_dt"] = (
            datetime.strptime(m["exited_at"], "%Y-%m-%d").date()
            if m["exited_at"]
            else None
        )

    # Members active 30 days ago
    active_30_days_ago = [
        m
        for m in formatted_members
        if m["joined_at_dt"]
        and m["joined_at_dt"] <= days_ago_30
        and (m["exited_at_dt"] is None or m["exited_at_dt"] > days_ago_30)
    ]
    # Members active today
    active_today = [
        m
        for m in formatted_members
        if m["joined_at_dt"]
        and m["joined_at_dt"] <= now
        and (m["exited_at_dt"] is None or m["exited_at_dt"] > now)
    ]
    # New members in last 30 days
    new_members_30d = [
        m
        for m in formatted_members
        if m["joined_at_dt"] and days_ago_30 < m["joined_at_dt"] <= now
    ]
    # New members in the previous 30 days (30-60 days ago)
    new_members_prev_30d = [
        m
        for m in formatted_members
        if m["joined_at_dt"] and days_ago_60 < m["joined_at_dt"] <= days_ago_30
    ]
    # Churned members in last 30 days
    churned_members_30d = [
        m
        for m in formatted_members
        if m["exited_at_dt"] and days_ago_30 < m["exited_at_dt"] <= now
    ]

    # Acquisition Rate = New Paying Members During Period / Starting Paying Members at Beginning of Period
    members_30d_ago = [
        m
        for m in formatted_members
        if m["joined_at_dt"] and m["joined_at_dt"] <= days_ago_30
    ]
    total_member_count_now = len(formatted_active_members)
    added_new_count = total_member_count_now - len(members_30d_ago)
    acquisition_rate_30d = (
        added_new_count / len(members_30d_ago) if members_30d_ago else 0
    )

    churn_rate_30d = (
        len(churned_members_30d) / len(active_30_days_ago) if active_30_days_ago else 0
    )
    # Retained revenue: sum of mrr for members who were active 30 days ago AND are still active today
    retained_members_30d = [m for m in active_30_days_ago if m in active_today]
    retained_revenue_30d = sum(m["mrr"] for m in retained_members_30d)
    net_new_members_30d = len(active_today) - len(active_30_days_ago)
    growth_rate_30d = (
        net_new_members_30d / len(active_30_days_ago) if active_30_days_ago else 0
    )

    print(f"  New members (last 30 days):       {len(new_members_30d)}")
    print(f"  Churned members (last 30 days):   {len(churned_members_30d)}")
    print(f"  Acquisition rate (30d):           {acquisition_rate_30d:.2%}")
    print(f"  Churn rate (30d):                 {churn_rate_30d:.2%}")
    print(f"  Retained revenue (30d):           ${retained_revenue_30d/100:,.2f}")
    print(f"  Growth rate (30d):                {growth_rate_30d:.2%}")
    print("=" * 40 + "\n")

    # --- Average LTV (Lifetime Value) ---
    print("Average LTV (Lifetime Value):")
    print(
        "  Formula: LTV = ARPU / Churn Rate, where ARPU = Total MRR / Total active paying members (members with price > 0), and Churn Rate is the 30-day churn rate (as a decimal, not percent). If churn rate is zero, LTV is infinite."
    )

    # ARPU (Average Revenue Per User, only paying members)
    paying_members = [
        m
        for m in formatted_members
        if m.get("is_active") and (m.get("price", 0) > 0 or m.get("mrr", 0) > 0)
    ]
    arpu = mrr / len(paying_members) if len(paying_members) > 0 else 0
    # Use 30-day churn rate as a decimal
    churn_rate_decimal = churn_rate_30d if churn_rate_30d else 0
    ltv = arpu / churn_rate_decimal if churn_rate_decimal else float("inf")

    print(f"  ARPU (paying):                 ${arpu/100:,.2f}")
    print(f"  Churn Rate (30d):     {churn_rate_30d:.4f}")
    # fyi format for this churn rate is: 0.014705882352941176
    if churn_rate_decimal:
        print(f"  LTV:                  ${ltv/100:,.2f}")
    else:
        print(f"  LTV:                  Infinite (no churn in last 30 days)")
    print("")

    # --- LTV (No Annual) ---
    total_active_paying_members_no_annual = len(
        [
            m
            for m in formatted_members
            if m.get("is_active")
            and m.get("price", 0) > 0
            and m.get("plan_type") != "annual"
        ]
    )
    arpu_no_annual = (
        mrr_no_annual / total_active_paying_members_no_annual
        if total_active_paying_members_no_annual
        else 0
    )
    # Churn rate (30d, no annual)
    churned_members_30d_no_annual = [
        m
        for m in formatted_members
        if m.get("exited_at_dt")
        and m.get("plan_type") != "annual"
        and days_ago_30 < m["exited_at_dt"] <= now
    ]
    active_30_days_ago_no_annual = [
        m
        for m in formatted_members
        if m.get("plan_type") != "annual"
        and m.get("joined_at_dt")
        and m["joined_at_dt"] <= days_ago_30
        and (m["exited_at_dt"] is None or m["exited_at_dt"] > days_ago_30)
    ]
    churn_rate_30d_no_annual = (
        len(churned_members_30d_no_annual) / len(active_30_days_ago_no_annual)
        if active_30_days_ago_no_annual
        else 0
    )
    ltv_no_annual = (
        arpu_no_annual / churn_rate_30d_no_annual
        if churn_rate_30d_no_annual
        else float("inf")
    )
    print(f"  ARPU (no annual):             ${arpu_no_annual/100:,.2f}")
    print(f"  Churn Rate (30d, no annual):  {churn_rate_30d_no_annual:.4f}")
    if churn_rate_30d_no_annual:
        print(f"  LTV (no annual):              ${ltv_no_annual/100:,.2f}")
    else:
        print(f"  LTV (no annual):              Infinite (no churn in last 30 days)")
    print("")

    ##### Upsert the Hypothetical Max section -------------------
    metrics = get_monthly_metrics_for_monthly_paying_members(formatted_members)
    print(metrics)
    hypotheticals = calculate_growth_metrics(
        current_members=metrics["current_members"],
        monthly_acquisition=metrics["monthly_acquisition"],
        monthly_churn_count=metrics["monthly_churn_count"],
        churn_rate=churn_rate_30d,
        average_price=metrics["average_price"],
        new_members_in_last_30d=metrics["new_members_in_last_30d"],
    )
    print(hypotheticals)
    ##### Upsert the Hypothetical Max section -------------------

    # Sync dashboard stats to the hud2_dashboard table
    sync_dashboard_to_db(
        supabase=supabase,
        community_slug=community_slug,
        month=now.strftime("%Y-%m-%d"),
        current_mrr=mrr,
        average_lifetime_value=ltv,
        current_mrr_no_annual=mrr_no_annual,
        average_lifetime_value_no_annual=ltv_no_annual,
        average_lifespan_no_annual=avg_lifespan_no_annual,
        total_members=total_active_members,
        total_paid_members=total_active_paying_members,
        # new_members_this_month=len(new_members_30d),
        new_members_30d=len(new_members_30d),
        new_members_prev_30d=len(new_members_prev_30d),
        acquisition_rate_this_month=acquisition_rate_30d,
        retained_revenue_this_month=retained_revenue_30d,
        members_churned_this_month=len(churned_members_30d),
        churn_rate_this_month=churn_rate_30d,
        growth_rate_this_month=growth_rate_30d,
        member_ceiling=hypotheticals["member_ceiling"],
        mrr_ceiling=hypotheticals["mrr_ceiling"],
        days_to_growth_ceiling=hypotheticals["days_to_growth_ceiling"],
        months_to_growth_ceiling=hypotheticals["months_to_growth_ceiling"],
        total_rev_ytd_recurring_only=total_rev_ytd_recurring_only,
    )
    ### Dashboard Section ------------------------

    #### Cohorts Section ------------------------
    cohort_retention = generate_cohort_retention_table(formatted_members, max_months=6)
    sync_cohort_to_db(cohort_retention, community_slug, supabase)
    #### Cohorts Section ------------------------

    #### Line Charts Section ------------------------
    metrics = calculate_monthly_metrics(formatted_members, community_slug)
    upsert_hud2_charts(supabase, metrics)
    #### Line Charts Section ------------------------

    #### Pie Charts Section -------------------
    distribution = get_level_distribution(formatted_members)
    print(distribution)

    active_and_cancelling_stats = get_active_and_cancelling_stats(
        [*formatted_active_members, *formatted_cancelling_members]
    )
    print(active_and_cancelling_stats)

    renewal_distribution = get_renewal_distribution(formatted_active_members)
    print(renewal_distribution)

    sync_pie_charts_to_db(
        distribution,
        active_and_cancelling_stats,
        renewal_distribution,
        community_slug,
        supabase,
    )
    # #### Pie Charts Section -------------------


def generate_cohort_retention_table(members, max_months=6):
    from collections import defaultdict
    from dateutil.relativedelta import relativedelta
    import math

    # Group members by cohort month (YYYY-MM)
    cohorts = defaultdict(list)
    for m in members:
        joined = m.get("joined_at")
        if not joined:
            continue
        try:
            cohort_month = datetime.strptime(joined, "%Y-%m-%d").strftime("%Y-%m")
            print(f"joined_at: {joined} -> cohort_month: {cohort_month}")
            cohorts[cohort_month].append(m)
        except Exception:
            continue

    # Find the latest joined_at date for max cohort span
    all_joined_dates = [m["joined_at"] for m in members if m.get("joined_at")]
    if not all_joined_dates:
        return []
    today = datetime.today()

    cohort_rows = []
    for cohort_month, cohort_members in sorted(cohorts.items()):
        cohort_start = datetime.strptime(cohort_month + "-01", "%Y-%m-%d")
        cohort_size = len(cohort_members)
        if cohort_size == 0:
            continue  # Skip empty cohorts
        row = {"cohort_month": cohort_month}
        for n in range(max_months):
            period_start = cohort_start + relativedelta(months=n)
            period_end = cohort_start + relativedelta(months=n + 1)
            if period_start > today:
                break
            if n == 0:
                still_active = cohort_size
                pct = 100.0
            else:
                still_active = 0
                for m in cohort_members:
                    joined_at = m.get("joined_at")
                    exited_at = m.get("exited_at")
                    try:
                        joined_dt = (
                            datetime.strptime(joined_at, "%Y-%m-%d")
                            if joined_at
                            else None
                        )
                        exited_dt = (
                            datetime.strptime(exited_at, "%Y-%m-%d")
                            if exited_at
                            else None
                        )
                    except Exception:
                        continue
                    if (
                        joined_dt
                        and joined_dt <= period_start
                        and (exited_dt is None or exited_dt >= period_end)
                    ):
                        still_active += 1
                pct = (still_active / cohort_size * 100) if cohort_size else 0
            row[f"month_{n}_count"] = still_active
            row[f"month_{n}_pct"] = round(pct, 2)
        cohort_rows.append(row)
    return cohort_rows


# Calculate average lifespan (excluding annual)
def average_lifespan_no_annual(formatted_members):
    lifespans = []
    for m in formatted_members:
        if m.get("plan_type") != "annual" and not m.get("is_active"):
            joined = m.get("joined_at")
            exited = m.get("exited_at")
            if joined and exited:
                try:
                    joined_dt = datetime.strptime(joined, "%Y-%m-%d")
                    exited_dt = datetime.strptime(exited, "%Y-%m-%d")
                    lifespan_days = (exited_dt - joined_dt).days
                    if lifespan_days > 0:
                        lifespans.append(lifespan_days)
                except Exception:
                    continue
    return sum(lifespans) / len(lifespans) if lifespans else 0


def calculate_mrr(members):
    # only for active members
    return sum(m["mrr"] for m in members if m.get("is_active"))


def calculate_mrr_no_annual(members):
    """
    Returns the sum of MRR for all members whose plan_type is not 'annual'.
    Args:
        members (list): List of member dicts, each with at least 'mrr' and 'plan_type' keys.
    Returns:
        float: Total MRR excluding annual plans.
    """
    # only for active members
    return sum(
        m["mrr"]
        for m in members
        if m.get("is_active") and m.get("plan_type") != "annual"
    )


def calculate_churn_past_30(formatted_members):
    now = datetime.utcnow().date()
    days_ago_30 = now - timedelta(days=30)
    churned_last_30 = [
        m
        for m in formatted_members
        if not m.get("is_active")
        and m.get("exited_at")
        and days_ago_30 <= datetime.strptime(m["exited_at"], "%Y-%m-%d").date() <= now
    ]
    return len(churned_last_30)


def calculate_churn_past_30_no_annual(formatted_members):
    now = datetime.utcnow().date()
    days_ago_30 = now - timedelta(days=30)
    churned_last_30_no_annual = [
        m
        for m in formatted_members
        if not m.get("is_active")
        and m.get("exited_at")
        and m.get("plan_type") != "annual"
        and days_ago_30 <= datetime.strptime(m["exited_at"], "%Y-%m-%d").date() <= now
    ]
    return len(churned_last_30_no_annual)


def count_active_30_days_ago(formatted_members, no_annual=False):
    now = datetime.utcnow().date()
    days_ago_30 = now - timedelta(days=30)
    return sum(
        1
        for m in formatted_members
        if (not no_annual or m.get("plan_type") != "annual")
        and m.get("joined_at")
        and datetime.strptime(m["joined_at"], "%Y-%m-%d").date() <= days_ago_30
        and (
            not m.get("exited_at")
            or datetime.strptime(m["exited_at"], "%Y-%m-%d").date() > days_ago_30
        )
    )


def churn_count_past_30(formatted_members, no_annual=False, debug=False):

    now = datetime.now(timezone.utc).date()
    days_ago_30 = now - timedelta(days=30)
    # Denominator: present at start of window
    starting = [
        m
        for m in formatted_members
        if m.get("joined_at")
        and datetime.strptime(m["joined_at"], "%Y-%m-%d").date() <= days_ago_30
        and (
            not m.get("exited_at")
            or datetime.strptime(m["exited_at"], "%Y-%m-%d").date() > days_ago_30
        )
        and (not no_annual or m.get("plan_type") != "annual")
    ]
    # Numerator: exited during window
    churned = [
        m
        for m in formatted_members
        if m.get("exited_at")
        and days_ago_30 <= datetime.strptime(m["exited_at"], "%Y-%m-%d").date() <= now
        and (not no_annual or m.get("plan_type") != "annual")
    ]
    if debug:
        print(
            f"{'Non-annual' if no_annual else 'All'} members - "
            f"Start of period ({days_ago_30}): {len(starting)}, "
            f"Churned during period: {len(churned)}"
        )
    churn_rate = (len(churned) / len(starting) * 100) if starting else 0
    return churn_rate


def get_level_distribution(members):
    """
    Returns a list of dicts for levels 1 through 8, each with:
      - 'level': int (1-8)
      - 'count': int (number of members at this level)
      - 'percent': float (percent of total members at this level, rounded to 2 decimals)
    """
    level_counts = {level: 0 for level in range(1, 9)}
    total = 0
    for m in members:
        level = m.get("level")
        if isinstance(level, int) and 1 <= level <= 8:
            level_counts[level] += 1
            total += 1
    result = []
    for level in range(1, 9):
        count = level_counts[level]
        percent = (count / total * 100) if total else 0.0
        result.append({"level": level, "count": count, "percent": round(percent, 2)})
    return result


def get_active_and_cancelling_stats(members):
    """
    Returns a dict with:
      - active_count: number of members with active_or_cancelling == 'active'
      - active_percent: percent of total that are active
      - cancelling_count: number of members with active_or_cancelling == 'cancelling'
      - cancelling_percent: percent of total that are cancelling
    """
    total = len(members)
    active_count = sum(1 for m in members if m.get("active_or_cancelling") == "active")
    cancelling_count = sum(
        1 for m in members if m.get("active_or_cancelling") == "cancelling"
    )
    active_percent = (active_count / total * 100) if total else 0.0
    cancelling_percent = (cancelling_count / total * 100) if total else 0.0
    return {
        "active_count": active_count,
        "active_percent": round(active_percent, 2),
        "cancelling_count": cancelling_count,
        "cancelling_percent": round(cancelling_percent, 2),
    }


def get_renewal_distribution(members):
    """
    Returns a list of dicts for months_renewed 0 through 7, and 8+ (as '8+'), each with:
      - 'months_renewed': int (0-7) or str '8+'
      - 'count': int (number of members at this renewal count)
      - 'percent': float (percent of total members at this renewal count, rounded to 2 decimals)
    """
    buckets = {i: 0 for i in range(0, 8)}
    buckets["8+"] = 0
    total = 0
    for m in members:
        mr = m.get("months_renewed")
        if mr is None or not isinstance(mr, int) or mr < 0:
            continue
        if mr >= 8:
            buckets["8+"] += 1
        else:
            buckets[mr] += 1
        total += 1
    result = []
    for i in range(0, 8):
        count = buckets[i]
        percent = (count / total * 100) if total else 0.0
        result.append(
            {"months_renewed": i, "count": count, "percent": round(percent, 2)}
        )
    count_8plus = buckets["8+"]
    percent_8plus = (count_8plus / total * 100) if total else 0.0
    result.append(
        {
            "months_renewed": "8+",
            "count": count_8plus,
            "percent": round(percent_8plus, 2),
        }
    )
    return result


def convert_member_to_hud2(
    member_obj, community_slug, force_active=True, billing_products=None
):
    import datetime
    import json

    # Helper to parse timestamps (int, float, or str)
    def parse_ts(ts):
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            # nanoseconds to seconds if too large
            if ts > 1e12:
                ts = ts / 1e9
            return datetime.datetime.utcfromtimestamp(ts).date()
        if isinstance(ts, str):
            try:
                # Try ISO format
                return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
            except Exception:
                try:
                    # Try parsing as int
                    return parse_ts(int(ts))
                except Exception:
                    return None
        return None

    def get_ltv(member):
        # member.metadata.mbsltv
        mbsltv = member.get("metadata", {}).get("mbsltv", 0)
        return mbsltv

    def get_months_renewed(member, monthly_or_annual):
        #    if monthly, then we will div by 30
        #    if annual, then we will div by 365
        #  find days between approvedAt and now
        #  then divide by 30 or 365
        #  then subtract 1 from that
        #  return that
        #  if approvedAt is None, return 0
        approved_at = parse_ts(member.get("member", {}).get("approvedAt"))
        if approved_at is None:
            return 0
        now = datetime.datetime.now().date()
        days_between = (now - approved_at).days
        if monthly_or_annual == "monthly":
            return days_between // 30
        return days_between // 365

    def get_level(member):
        metadata = member.get("metadata", {})
        spdata = metadata.get("spData", {})
        # Only parse if it's a string
        if isinstance(spdata, str):
            try:
                spdata = json.loads(spdata)
            except Exception:
                spdata = {}
        level = spdata.get("lv", "")
        return level

    # Find churn date (exited_at)
    def get_churn_date(member):
        # NOTE: dont use "removedAt" for this...
        m = member.get("member", {})
        if m.get("churned"):
            return parse_ts(m.get("churned"))
        return None

    # New plan_type, price, and mrr logic using billingProductId and billing_products
    def get_plan_type_price_and_mrr(member, billing_products):
        m = member.get("member", {})
        billing_product_id = m.get("billingProductId")
        if (
            billing_product_id
            and billing_products
            and billing_product_id in billing_products
        ):
            bp = billing_products[billing_product_id]
            interval = bp.get("interval", "unknown")
            # Normalize interval to 'annual', 'monthly', or 'one_time'
            if interval in ["year", "annual"]:
                plan_type = "annual"
            elif interval in ["month", "monthly"]:
                plan_type = "monthly"
            elif interval in ["one_time", "one-time"]:
                plan_type = "one_time"
            else:
                plan_type = "unknown"
            price = bp.get("price", 0)
            try:
                price = float(price)
            except Exception:
                price = 0.0
            # MRR logic: always monthly
            if plan_type == "annual":
                mrr = price / 12
            elif plan_type == "monthly":
                mrr = price
            else:
                mrr = 0.0
            return plan_type, price, mrr
        return "unknown", 0.0, 0.0

    joined_at_date = parse_ts(member_obj.get("member", {}).get("approvedAt"))
    exited_at_date = get_churn_date(member_obj)
    # Convert to ISO string if not None
    joined_at = joined_at_date.isoformat() if joined_at_date else None
    exited_at = exited_at_date.isoformat() if exited_at_date else None
    is_active = exited_at is None
    if force_active:
        is_active = True
    plan_type, price, mrr = get_plan_type_price_and_mrr(member_obj, billing_products)

    return {
        "id": member_obj.get("id"),
        "joined_at": joined_at,
        "exited_at": exited_at,
        "is_active": is_active,
        "mrr": mrr,
        "price": price,
        "plan_type": plan_type,
        "community_slug": community_slug,
        "active_or_cancelling": "active",  # always false, because we set it to true if needed outside of this fn
        "level": get_level(member_obj),
        "months_renewed": get_months_renewed(member_obj, plan_type),
    }


def sync_hud2_members_to_db(formatted_members, community_slug, supabase):
    """
    Syncs the given members to the hud2_members table in Supabase for the given community_slug.
    Deletes any rows for this community_slug not in the new member id list.
    Adds logging and print statements with emojis for visibility.
    """
    print("\n🚀 Starting HUD2 member sync for community_slug:", community_slug)
    print(f"👥 Members to sync: {len(formatted_members)}")

    # Format all members
    member_ids = set(m["id"] for m in formatted_members)
    print(f"📝 Formatted member IDs: {member_ids}")

    # Fetch existing IDs from DB
    print("🔍 Fetching existing member IDs from DB...")
    existing = (
        supabase.table("hud2_members")
        .select("id")
        .eq("community_slug", community_slug)
        .execute()
        .data
    )
    existing_ids = set(row["id"] for row in existing)
    print(f"📦 Existing member IDs in DB: {existing_ids}")

    # Find IDs to delete
    to_delete = existing_ids - member_ids
    print(f"🗑️ Member IDs to delete: {to_delete}")
    for member_id in to_delete:
        print(f"   ➡️ Deleting member ID: {member_id}")
        supabase.table("hud2_members").delete().eq("community_slug", community_slug).eq(
            "id", member_id
        ).execute()

    # Upsert (insert/update) all formatted members
    print(f"⬆️ Upserting {len(formatted_members)} members...")
    for member in formatted_members:
        print(
            f"   💾 Upserting member: {member['id']} (active: {member['is_active']}, mrr: {member['mrr']}, plan: {member['plan_type']})"
        )
        supabase.table("hud2_members").upsert(member).execute()

    print("✅ HUD2 member sync complete!\n")


def sync_cohort_to_db(cohort_retention_rows, community_slug, supabase):
    """
    Syncs the given cohort retention rows to the hud2_cohort table in Supabase for the given community_slug.
    Deletes any rows for this community_slug not in the new cohort set (by cohort_month and month_index).
    Adds logging and print statements for visibility.
    """
    print(f"\n🚀 Starting HUD2 cohort sync for community_slug: {community_slug}")
    print(f"📊 Cohort rows to sync: {len(cohort_retention_rows)}")

    # Prepare all new cohort keys (cohort_month, month_index)
    new_keys = set()
    upsert_rows = []
    for row in cohort_retention_rows:
        cohort_month = row["cohort_month"]
        for n in range(6):  # max_months is 6 in the generator
            count_key = f"month_{n}_count"
            pct_key = f"month_{n}_pct"
            if count_key in row and pct_key in row:
                upsert_rows.append(
                    {
                        "community_slug": community_slug,
                        "cohort_month": f"{cohort_month}-01",  # always first day of month
                        "month_index": n,
                        "count": row[count_key],
                        "pct": row[pct_key],
                    }
                )
                new_keys.add((f"{cohort_month}-01", n))

    print(f"📝 New cohort keys: {new_keys}")

    # Fetch existing keys from DB
    print("🔍 Fetching existing cohort keys from DB...")
    existing = (
        supabase.table("hud2_cohort")
        .select("cohort_month", "month_index")
        .eq("community_slug", community_slug)
        .execute()
        .data
    )
    existing_keys = set((row["cohort_month"], row["month_index"]) for row in existing)
    print(f"📦 Existing cohort keys in DB: {existing_keys}")

    # Find keys to delete
    to_delete = existing_keys - new_keys
    print(f"🗑️ Cohort keys to delete: {to_delete}")
    for cohort_month, month_index in to_delete:
        print(f"   ➡️ Deleting cohort row: ({cohort_month}, {month_index})")
        supabase.table("hud2_cohort").delete().eq("community_slug", community_slug).eq(
            "cohort_month", cohort_month
        ).eq("month_index", month_index).execute()

    # Delete all existing rows for this community_slug
    print(f"🗑️ Deleting all cohort rows for community_slug: {community_slug}")
    supabase.table("hud2_cohort").delete().eq(
        "community_slug", community_slug
    ).execute()

    # Insert all new rows
    print(f"⬆️ Inserting {len(upsert_rows)} cohort rows...")
    supabase.table("hud2_cohort").insert(upsert_rows).execute()

    print("✅ HUD2 cohort sync complete!\n")


def sync_dashboard_to_db(
    supabase,
    community_slug,
    month,
    current_mrr,
    average_lifetime_value,
    current_mrr_no_annual,
    average_lifetime_value_no_annual,
    average_lifespan_no_annual,
    total_members,
    total_paid_members,
    new_members_30d,
    new_members_prev_30d,
    acquisition_rate_this_month,
    retained_revenue_this_month,
    members_churned_this_month,
    churn_rate_this_month,
    growth_rate_this_month,
    member_ceiling,
    mrr_ceiling,
    days_to_growth_ceiling,
    months_to_growth_ceiling,
    total_rev_ytd_recurring_only,
):
    import datetime

    """
    Syncs the given dashboard stats to the hud2_dashboard table in Supabase for the given community_slug and month.
    Deletes any rows for this community_slug and month, then inserts the new stats.
    """
    print(
        f"\n🚀 Starting HUD2 dashboard sync for community_slug: {community_slug}, month: {month}"
    )
    # Delete any existing row for this community_slug and month
    print(
        f"🗑️ Deleting existing dashboard row for community_slug='{community_slug}', month='{month}'..."
    )
    supabase.table("hud2_dashboard").delete().eq("community_slug", community_slug).eq(
        "month", month
    ).execute()

    def safe_int(val):
        if val is None:
            return None
        if isinstance(val, float) and (math.isinf(val) or math.isnan(val)):
            return None
        return int(val)

    dashboard_row = {
        "community_slug": community_slug,
        "month": month,
        "current_mrr": safe_int(current_mrr),
        "average_lifetime_value": safe_int(average_lifetime_value),
        "current_mrr_no_annual": safe_int(current_mrr_no_annual),
        "average_lifetime_value_no_annual": safe_int(average_lifetime_value_no_annual),
        "average_lifespan_no_annual": (
            float(average_lifespan_no_annual)
            if average_lifespan_no_annual is not None
            else None
        ),
        "total_members": safe_int(total_members),
        "total_paid_members": safe_int(total_paid_members),
        "new_members_past_30": safe_int(new_members_30d),
        "new_members_prev_30": safe_int(new_members_prev_30d),
        "acquisition_rate_this_month": (
            float(acquisition_rate_this_month)
            if acquisition_rate_this_month is not None
            else None
        ),
        "retained_revenue_this_month": safe_int(retained_revenue_this_month),
        "members_churned_this_month": safe_int(members_churned_this_month),
        "churn_rate_this_month": (
            float(churn_rate_this_month) if churn_rate_this_month is not None else None
        ),
        "growth_rate_this_month": (
            float(growth_rate_this_month)
            if growth_rate_this_month is not None
            else None
        ),
        "member_ceiling": safe_int(member_ceiling),
        "mrr_ceiling": safe_int(mrr_ceiling),
        "days_to_growth_ceiling": safe_int(days_to_growth_ceiling),
        "months_to_growth_ceiling": safe_int(months_to_growth_ceiling),
        "total_rev_ytd_recurring_only": safe_int(total_rev_ytd_recurring_only),
    }
    print(f"⬆️ Inserting dashboard row: {dashboard_row}")
    supabase.table("hud2_dashboard").insert(dashboard_row).execute()
    print("✅ HUD2 dashboard sync complete!\n")


def sync_pie_charts_to_db(
    distribution,
    active_and_cancelling_stats,
    renewal_distribution,
    community_slug,
    supabase,
):
    """
    Syncs the pie chart stats to the hud2_chart_pie table in Supabase for the given community_slug.
    Deletes any rows for this community_slug, then inserts the new stats.
    """
    print(f"\n🚀 Starting HUD2 pie chart sync for community_slug: {community_slug}")
    # Delete any existing row for this community_slug
    print(f"🗑️ Deleting existing pie chart row for community_slug='{community_slug}'...")
    supabase.table("hud2_chart_pie").delete().eq(
        "community_slug", community_slug
    ).execute()

    # Map distribution (levels 1-8) to snake_case
    pie_row = {
        "community_slug": community_slug,
    }
    for d in distribution:
        level = d["level"]
        pie_row[f"level_{level}_count"] = int(d["count"])
        pie_row[f"level_{level}_pct"] = float(d["percent"])

    # Map active/cancelling to snake_case
    pie_row["active_count"] = int(active_and_cancelling_stats["active_count"])
    pie_row["active_pct"] = float(active_and_cancelling_stats["active_percent"])
    pie_row["cancelling_count"] = int(active_and_cancelling_stats["cancelling_count"])
    pie_row["cancelling_pct"] = float(active_and_cancelling_stats["cancelling_percent"])

    # Map renewal buckets (0-7, 8+) to snake_case
    for r in renewal_distribution:
        months = r["months_renewed"]
        if months == "8+":
            pie_row["renewal_8_plus_count"] = int(r["count"])
            pie_row["renewal_8_plus_pct"] = float(r["percent"])
        else:
            pie_row[f"renewal_{months}_count"] = int(r["count"])
            pie_row[f"renewal_{months}_pct"] = float(r["percent"])

    print(f"⬆️ Inserting pie chart row: {pie_row}")
    supabase.table("hud2_chart_pie").insert(pie_row).execute()
    print("✅ HUD2 pie chart sync complete!\n")


def get_monthly_metrics_for_monthly_paying_members(members):
    """
    Returns a dict with:
      - current_members: count of active, monthly, paying users
      - monthly_acquisition: count of new monthly-paying members in the last 30 days
      - monthly_churn_count: count of monthly-paying users who canceled in the last 30 days
      - churn_rate: churn rate for monthly-paying users (churned in last 30 days / active 30 days ago)
      - average_price: average price of monthly plan (for current active monthly-paying users)
    All calculations are rolling 30 days.
    """
    from datetime import datetime, timedelta

    now = datetime.utcnow().date()
    days_ago_30 = now - timedelta(days=30)

    # Only monthly, paying users
    monthly_paying = [
        m for m in members if m.get("plan_type") == "monthly" and m.get("price", 0) > 0
    ]

    # Current members: active, monthly, paying
    current_members = [m for m in monthly_paying if m.get("is_active")]

    # New members in last 30 days
    monthly_acquisition = [
        m
        for m in monthly_paying
        if m.get("joined_at")
        and days_ago_30 < datetime.strptime(m["joined_at"], "%Y-%m-%d").date() <= now
    ]

    # Churned members in last 30 days
    monthly_churned = [
        m
        for m in monthly_paying
        if not m.get("is_active")
        and m.get("exited_at")
        and days_ago_30 < datetime.strptime(m["exited_at"], "%Y-%m-%d").date() <= now
    ]

    # Members active 30 days ago (monthly, paying)
    active_30_days_ago = [
        m
        for m in monthly_paying
        if m.get("joined_at")
        and datetime.strptime(m["joined_at"], "%Y-%m-%d").date() <= days_ago_30
        and (
            not m.get("exited_at")
            or datetime.strptime(m["exited_at"], "%Y-%m-%d").date() > days_ago_30
        )
    ]

    churn_rate = (
        (len(monthly_churned) / len(active_30_days_ago)) if active_30_days_ago else 0.0
    )
    # FYI churn is in this format: 0.014705882352941176

    # Average price for current active monthly-paying users
    avg_price = (
        sum(m["price"] for m in current_members) / len(current_members)
        if current_members
        else 0.0
    )

    new_members_in_last_30d = []
    for m in members:
        if (
            m.get("joined_at")
            and days_ago_30
            < datetime.strptime(m["joined_at"], "%Y-%m-%d").date()
            <= now
        ):
            new_members_in_last_30d.append(m)

    return {
        "current_members": len(current_members),
        "monthly_acquisition": len(monthly_acquisition),
        "monthly_churn_count": len(monthly_churned),
        "churn_rate": churn_rate,
        "average_price": avg_price,
        "new_members_in_last_30d": len(new_members_in_last_30d),
    }


def calculate_growth_metrics(
    current_members: int,
    monthly_acquisition: float,
    monthly_churn_count: float,
    churn_rate: float,
    # fyi format for this churn rate is: 0.014705882352941176
    average_price: float,
    ceiling_pct_growth: float = 0.95,
    ceiling_pct_member: float = 0.99,
    new_members_in_last_30d: int = 0,
) -> Dict[str, float]:
    """
    Returns a dict with:
        - member_ceiling: new members in last 30d / churn rate
        - growth_rate: net monthly growth rate (decimal)
        - mrr_ceiling: max possible MRR
        - potential_max_revenue: max possible annual revenue
        - days_to_growth_ceiling: days to reach 95% of member ceiling
        - days_to_member_ceiling: days to reach 99% of member ceiling
        - months_to_growth_ceiling: months to reach 95% of member ceiling
    """
    if churn_rate == 0:
        member_ceiling = None
        mrr_ceiling = None
        potential_max_revenue = None
        days_to_growth_ceiling = None
        days_to_member_ceiling = None
        months_to_growth_ceiling = None
        growth_rate = 0.0
    else:
        member_ceiling = monthly_acquisition / churn_rate
        mrr_ceiling = member_ceiling * average_price
        potential_max_revenue = mrr_ceiling * 12
        # Net monthly growth rate (ignoring contraction/expansion revenue)
        growth_rate = (
            (monthly_acquisition - monthly_churn_count) / current_members
            if current_members > 0
            else 0.0
        )

        # Time to reach X% of ceiling: t = -ln(1 - pct) / (acquisition + churn) * churn_rate
        # Actually, for classic SaaS: N(t) = ceiling - (ceiling - N0) * exp(-churn_rate * t)
        # Solve for t: N(t) = ceiling * pct => t = -ln((ceiling - N(t)) / (ceiling - N0)) / churn_rate
        def time_to_pct_ceiling(pct: float) -> float:
            if member_ceiling == 0 or current_members >= member_ceiling:
                return 0.0
            ratio = (member_ceiling - pct * member_ceiling) / (
                member_ceiling - current_members
            )
            if ratio <= 0:
                return 0.0
            return -math.log(ratio) / churn_rate

        months_to_growth_ceiling = time_to_pct_ceiling(ceiling_pct_growth)
        days_to_growth_ceiling = months_to_growth_ceiling * 30.44
        days_to_member_ceiling = time_to_pct_ceiling(ceiling_pct_member) * 30.44
    return {
        "member_ceiling": member_ceiling,
        "growth_rate": growth_rate,
        "mrr_ceiling": mrr_ceiling,
        "potential_max_revenue": potential_max_revenue,
        "days_to_growth_ceiling": days_to_growth_ceiling,
        "days_to_member_ceiling": days_to_member_ceiling,
        "months_to_growth_ceiling": months_to_growth_ceiling,
    }


def get_total_rev_ytd_recurring_only(members):
    """
    Returns the total revenue made this year from recurring members.
    For each member:
      - If plan_type is 'annual', use their 'price' as their total revenue for the year (counted once if they were active at any point this year).
      - If plan_type is not 'annual', for each month in the current year they were active, add their MRR for that month.
    This matches the business logic: annuals are counted as their full price for the year, not broken down by month.
    """
    from datetime import datetime, date

    total_rev = 0.0
    now = datetime.now().date()
    current_year = now.year
    first_of_year = date(current_year, 1, 1)

    for m in members:
        joined_at = m.get("joined_at")
        exited_at = m.get("exited_at")
        mrr = m.get("mrr", 0.0)
        price = m.get("price", 0.0)
        plan_type = m.get("plan_type")
        if not joined_at:
            continue
        joined_at_dt = datetime.strptime(joined_at, "%Y-%m-%d").date()
        exited_at_dt = (
            datetime.strptime(exited_at, "%Y-%m-%d").date() if exited_at else None
        )

        # --- Annual plan logic ---
        if plan_type == "annual":
            # Only count if they were active at any point this year
            # (joined before end of year and exited after start of year or still active)
            active_start = joined_at_dt <= now and (
                exited_at_dt is None or exited_at_dt >= first_of_year
            )
            if active_start:
                total_rev += price
            continue

        # --- Monthly/other recurring logic ---
        if mrr == 0:
            continue
        # Determine the first and last month to count for this member in the current year
        start = max(joined_at_dt, first_of_year)
        end = (
            exited_at_dt if exited_at_dt and exited_at_dt.year == current_year else now
        )
        if end.year < current_year:
            continue  # Exited before this year
        if start.year > current_year:
            continue  # Joined after this year
        # Count months from start to end (inclusive, but only in current year)
        month_count = (end.year - start.year) * 12 + (end.month - start.month) + 1
        # If start is before this year, only count months in this year
        if joined_at_dt.year < current_year:
            month_count = end.month
        # If exited this year, but joined before this year
        if (
            exited_at_dt
            and exited_at_dt.year == current_year
            and joined_at_dt.year < current_year
        ):
            month_count = exited_at_dt.month
        # If joined and exited both this year
        if (
            exited_at_dt
            and exited_at_dt.year == current_year
            and joined_at_dt.year == current_year
        ):
            month_count = exited_at_dt.month - joined_at_dt.month + 1
        # If joined this year and still active
        if not exited_at_dt and joined_at_dt.year == current_year:
            month_count = now.month - joined_at_dt.month + 1
        # If joined before this year and still active
        if not exited_at_dt and joined_at_dt.year < current_year:
            month_count = now.month
        # Clamp to at least 0
        month_count = max(0, month_count)
        total_rev += mrr * month_count
    return total_rev


if __name__ == "__main__":
    pass
