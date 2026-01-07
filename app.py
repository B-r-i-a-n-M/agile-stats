import streamlit as st
import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import base64
import urllib.parse
import plotly.express as px
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Database Setup ---
DB_FILE = "sprint_stats.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Table: sprint_capacities
    c.execute('''
        CREATE TABLE IF NOT EXISTS sprint_capacities (
            sprint_id INTEGER PRIMARY KEY,
            sprint_name TEXT,
            planned_capacity REAL,
            final_capacity REAL
        )
    ''')

    # Table: sprint_metrics
    c.execute('''
        CREATE TABLE IF NOT EXISTS sprint_metrics (
            sprint_id INTEGER PRIMARY KEY,
            sprint_name TEXT,
            velocity REAL,
            completed_planned REAL,
            completed_unplanned REAL,
            carryover_pct REAL,
            bugs_in INTEGER,
            bugs_out INTEGER,
            completion_pct_total REAL,
            planned_pct REAL,
            unplanned_pct REAL DEFAULT 0,
            planned_sp REAL DEFAULT 0,
            unplanned_sp REAL DEFAULT 0,
            task_count_completed INTEGER DEFAULT 0,
            task_count_incomplete INTEGER DEFAULT 0,
            task_count_total INTEGER DEFAULT 0,
            bugs_out_sp REAL DEFAULT 0, 
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Add columns if they don't exist (for existing DBs)
    try:
        c.execute('ALTER TABLE sprint_metrics ADD COLUMN planned_sp REAL DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE sprint_metrics ADD COLUMN unplanned_sp REAL DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE sprint_metrics ADD COLUMN sprint_name TEXT')
    except:
        pass
    try:
        c.execute('ALTER TABLE sprint_metrics ADD COLUMN unplanned_pct REAL DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE sprint_metrics ADD COLUMN task_count_completed INTEGER DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE sprint_metrics ADD COLUMN task_count_incomplete INTEGER DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE sprint_metrics ADD COLUMN task_count_total INTEGER DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE sprint_metrics ADD COLUMN bugs_out_sp REAL DEFAULT 0')
    except:
        pass
    # Table: app_config
    c.execute('''
        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    conn.close()

def save_config(key, value):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO app_config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, str(value)))
    conn.commit()
    conn.close()

def get_config(key, default=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT value FROM app_config WHERE key = ?', (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else default

def delete_sprint_data(sprint_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('DELETE FROM sprint_metrics WHERE sprint_id = ?', (sprint_id,))
    c.execute('DELETE FROM sprint_capacities WHERE sprint_id = ?', (sprint_id,))
    conn.commit()
    conn.close()

def save_capacity(sprint_id, sprint_name, planned, final):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO sprint_capacities (sprint_id, sprint_name, planned_capacity, final_capacity)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(sprint_id) DO UPDATE SET
            sprint_name=excluded.sprint_name,
            planned_capacity=excluded.planned_capacity,
            final_capacity=excluded.final_capacity
    ''', (sprint_id, sprint_name, planned, final))
    conn.commit()
    conn.close()

def get_capacity(sprint_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT planned_capacity, final_capacity FROM sprint_capacities WHERE sprint_id = ?', (sprint_id,))
    row = c.fetchone()
    conn.close()
    return row if row else (0.0, 0.0)

def save_metrics(sprint_id, sprint_name, metrics):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO sprint_metrics (
            sprint_id, sprint_name, velocity, completed_planned, completed_unplanned, 
            carryover_pct, bugs_in, bugs_out, completion_pct_total, planned_pct,
            planned_sp, unplanned_sp, unplanned_pct,
            task_count_completed, task_count_incomplete, task_count_total, bugs_out_sp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sprint_id) DO UPDATE SET
            sprint_name=excluded.sprint_name,
            velocity=excluded.velocity,
            completed_planned=excluded.completed_planned,
            completed_unplanned=excluded.completed_unplanned,
            carryover_pct=excluded.carryover_pct,
            bugs_in=excluded.bugs_in,
            bugs_out=excluded.bugs_out,
            completion_pct_total=excluded.completion_pct_total,
            planned_pct=excluded.planned_pct,
            unplanned_pct=excluded.unplanned_pct,
            planned_sp=excluded.planned_sp,
            unplanned_sp=excluded.unplanned_sp,
            task_count_completed=excluded.task_count_completed,
            task_count_incomplete=excluded.task_count_incomplete,
            task_count_total=excluded.task_count_total,
            bugs_out_sp=excluded.bugs_out_sp
    ''', (
        sprint_id,
        sprint_name,
        metrics['velocity'], 
        metrics['completed_planned'], 
        metrics['completed_unplanned'],
        metrics['carryover_pct'], 
        metrics['bugs_in'], 
        metrics['bugs_out'],
        metrics['completion_pct_total'], 
        metrics['planned_pct'],
        metrics.get('planned_sp', 0),
        metrics.get('unplanned_sp', 0),
        metrics.get('unplanned_pct', 0.0),
        metrics.get('task_count_completed', 0),
        metrics.get('task_count_incomplete', 0),
        metrics.get('task_count_total', 0),
        metrics.get('bugs_out_sp', 0.0)
    ))
    conn.commit()
    conn.close()

def get_all_metrics():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql('SELECT * FROM sprint_metrics', conn)
    conn.close()
    return df

# --- Jira API Functions ---
def get_auth_header(email, token):
    creds = f"{email}:{token}"
    encoded = base64.b64encode(creds.encode("utf-8")).decode("utf-8")
    return {"Authorization": f"Basic {encoded}", "Content-Type": "application/json"}

def get_sprints(domain, board_id, auth_header, limit=20):
    url = f"https://{domain}/rest/agile/1.0/board/{board_id}/sprint"
    
    # 1. First fetch to get the 'total' count
    try:
        params = {"state": "active,closed,future", "maxResults": 1}
        r = requests.get(url, headers=auth_header, params=params)
        r.raise_for_status()
        total = r.json().get('total', 0)
    except Exception as e:
        print(f"Error fetching sprint total: {e}")
        return []

    if total == 0:
        return []

    # 2. Calculate startAt to get the LATEST sprints
    # If total=100 and limit=20, we want to start at 80
    start_at = max(0, total - limit)
    
    sprints = []
    while len(sprints) < limit:
        # Request in chunks
        fetch_count = min(50, limit - len(sprints))
        params = {"state": "active,closed,future", "maxResults": fetch_count, "startAt": start_at}
        try:
            r = requests.get(url, headers=auth_header, params=params)
            r.raise_for_status()
            data = r.json()
            values = data.get('values', [])
            if not values:
                break
            sprints.extend(values)
            start_at += len(values)
            if start_at >= total:
                break
        except Exception as e:
            print(f"Error fetching sprint chunk: {e}")
            break
            
    # Sort by ID descending (most recent first)
    sprints.sort(key=lambda x: x['id'], reverse=True)
    return sprints

def get_team_members(domain, team_id, auth_header):
    # This is a bit tricky. The user mentioned Team ID.
    # We might need to use the generic user search or a specific teams API.
    # Assuming standard Jira Cloud logic, sometimes 'teams' is handled differently.
    # However, 'assignee' usually just needs accountId. 
    # If the user provided a Team ID, we should try to fetch members of that team to filter 'Bugs In'.
    # Note: access to team members via API usually requires specific permissions/APIs (like teams-api.atlassian.com).
    # Since we need to keep it simple and within standard Jira auth if possible, let's try a direct approach.
    # If standard Jira API doesn't easily expose team members without 3rd party plugins (like Tempo/Portfolio),
    # we might strictly rely on the users being part of the 'assignee' field in the fetched issues.
    # BUT, prompt says "Fetch team members using the Atlassian Teams API".
    
    # We will try the Teams API generic endpoint.
    # This endpoint often requires a different base URL: https://api.atlassian.com/teams/v1/org/{orgId}/teams/{teamId}/members
    # But usually "Domain" is like "mycompany.atlassian.net".
    # Let's try to infer or ask. For now, we'll try to use the issues to infer team, or assume all assignees in the board are relevant if this fails.
    # Actually, let's look for a cleaner way: The prompt explicitly says fetch members.
    # Let's try a common known endpoint for Teams in Jira Cloud if available, or skip with a warning if exact endpoint is obscure.
    # Better approach given the constraints: We can't easily guess the 'Org ID' for the Teams API.
    # However, we can use the /rest/api/3/user/search?query=... if we had names.
    # Let's stick to identifying team members from the issues themselves if we can't hit the API, 
    # OR we just implement a placeholder for this specific team filter if API fails.
    
    # Update: The prompt gave a specific UUID for Team ID.
    # Let's try https://api.atlassian.com/ex/jira/{cloudId}/... wait, standard Jira API is on the domain.
    # We'll try to fetch all assignees from the sprint issues and assume they must be filtered by the "Team" field if it exists on the issue,
    # OR we just trust the prompt's request for "Atlassian Teams API".
    # Since I don't have the full context of their Atlassian setup (Org ID etc), I will implement a robust fallback:
    # We will just fetch the 'Bugs In' regardless of assignee first (marked as warning), or try to filter if I can.
    # Actually, a common pattern for "Team" in Jira is a custom field.
    # Let's simplify: We will filter Bugs In by *Assignee* being in the list of people who worked on *other things* in the sprint?
    # No, prompt says: "Fetch team members ... or filter by 'assignee' if needed".
    # Let's allow the user to input a comma-separated list of Account IDs or Emails if the API fails?
    # No, automation is key.
    # Let's try to just check if the assignee was active in the sprint?
    # Let's assume for this code that we check if the assignee is present in the list of assignees for the *sprint's issues*.
    # That might be a safe "Team" proxy.
    pass

def get_sprint_issues(domain, sprint_id, auth_header, sp_field_id):
    url = f"https://{domain}/rest/agile/1.0/sprint/{sprint_id}/issue"
    # Dynamic fields
    fields_to_fetch = [
        "summary", "status", "issuetype", "created", "resolutiondate", 
        "assignee", "changelog", sp_field_id,
        "customfield_10020", "issuekey" # Sprint
    ]
    fields_param = ",".join(fields_to_fetch)

    # Fetch ALL issues, filter sub-tasks in python
    params = {
        # "jql": "issuekey is not EMPTY", # Optional, usually implied
        "fields": fields_param,
        "expand": "changelog",
        "maxResults": 1000
    }
    
    sprint_info_url = f"https://{domain}/rest/agile/1.0/sprint/{sprint_id}"
    sprint_info = requests.get(sprint_info_url, headers=auth_header).json()
    
    issues = []
    start_at = 0
    while True:
        p = params.copy()
        p['startAt'] = start_at
        r = requests.get(url, headers=auth_header, params=p)
        r.raise_for_status()
        data = r.json()
        issues.extend(data.get('issues', []))
        if start_at + len(data.get('values', data.get('issues', []))) >= data.get('total', 0):
            break
        start_at += len(data.get('values', data.get('issues', [])))
        
    return sprint_info, issues

def get_bugs_in(domain, sprint_end_iso, team_id, auth_header):
    """
    Fetches bugs transitioned to 'Triaged' within the sprint window.
    Window: Tuesday (planning day, sprint_end - 13 days) to Monday (day before close, sprint_end - 1 day)
    """
    # Parse sprint end
    if not sprint_end_iso:
        end_dt = datetime.now()
    else:
        end_dt = parse_date(sprint_end_iso)
        if not end_dt:
            end_dt = datetime.now()

    # Calculate window: sprint closes Tuesday, window is Tuesday -13 days to Monday -1 day
    window_end = (end_dt - timedelta(days=1)).strftime("%Y-%m-%d")   # Monday before close
    window_start = (end_dt - timedelta(days=13)).strftime("%Y-%m-%d") # Tuesday (planning)
    
    # Use "Team[Team]" syntax as per working Slack integration
    jql = f'type = Bug AND "Team[Team]" = "{team_id}" AND status CHANGED TO "Triaged" DURING ("{window_start}", "{window_end}")'
    
    # Use NEW API endpoint (old /search deprecated as of 2024)
    url = f"https://{domain}/rest/api/3/search/jql"
    params = {
        "jql": jql,
        "maxResults": 1000
    }
    
    r = requests.get(url, headers=auth_header, params=params)
    if r.status_code == 200:
        return r.json().get('issues', [])
    else:
        print(f"Bugs In JQL failed ({r.status_code}): {jql}")
        try:
            error_msg = r.json().get('errorMessages', [])
            print(f"Error details: {error_msg}")
        except:
            pass
    return []

def get_board_done_statuses(domain, board_id, auth_header):
    url = f"https://{domain}/rest/agile/1.0/board/{board_id}/configuration"
    try:
        r = requests.get(url, headers=auth_header)
        r.raise_for_status()
        data = r.json()
        
        # Get the columns
        columns = data.get('columnConfig', {}).get('columns', [])
        if not columns:
            return []
            
        # Assuming the Right-Most column is "Done"
        done_column = columns[-1]
        statuses = [s['id'] for s in done_column.get('statuses', [])]
        
        # We need status NAMES or IDs? 
        # The issue fields return status object with name and id.
        # Let's verify what the config returns. It usually returns 'id' (status id).
        # But our main loop might rely on names or we need to map ids.
        # Let's return a set of Status IDs for robustness.
        return set(statuses)
    except Exception as e:
        print(f"Error fetching board config: {e}")
        return set()

def get_jira_fields(domain, auth_header):
    url = f"https://{domain}/rest/api/3/field"
    try:
        r = requests.get(url, headers=auth_header)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return [{"error": str(e)}]

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except:
        try:
            base = date_str.rsplit('+', 1)[0].rsplit('-', 1)[0]
            if '.' in base:
                base = base.split('.')[0]
            return datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")
        except:
            return None

def get_status_id_at_date(issue, target_date):
    """
    Reconstructs the status ID of the issue at a specific point in time
    using the changelog.
    """
    if not target_date:
        return issue['fields']['status']['id']
    
    # Current status ID (fallback if no history found relative to date)
    current_status_id = issue['fields']['status']['id']
    
    histories = issue.get('changelog', {}).get('histories', [])
    if not histories:
        return current_status_id

    # Sort histories by created date ascending
    sorted_histories = sorted(histories, key=lambda x: x['created'])
    
    status_changes = []
    for h in sorted_histories:
        for item in h['items']:
            if item['field'] == 'status':
                status_changes.append({
                    'date': parse_date(h['created']),
                    'from': item['from'], # ID
                    'to': item['to']      # ID
                })
    
    if not status_changes:
        return current_status_id
        
    # Replay logic:
    # We want status at T.
    # 1. Start with initial status (from first change)
    # 2. Apply all changes where date <= T
    
    replayed_status_id = status_changes[0]['from']
    
    # Check if target date is BEFORE the first change?
    # If so, status was initial. Good.
    
    for change in status_changes:
        if change['date'] <= target_date:
            replayed_status_id = change['to']
        else:
            # Change happened after target date
            break
            
    return replayed_status_id

def calculate_stats(sprint_info, issues, bugs_in_issues, planned_capacity, final_capacity, sp_field_id, done_status_ids):
    sprint_start_str = sprint_info.get('startDate')
    sprint_end_str = sprint_info.get('completeDate')
    
    sprint_start = parse_date(sprint_start_str)
    sprint_end = parse_date(sprint_end_str)
    
    if not sprint_start:
        return {}, []

    completed_planned = 0.0
    completed_unplanned = 0.0
    incomplete_count = 0
    all_sprint_tasks_count = 0
    completed_outside_count = 0
    bugs_out_count = 0
    bugs_out_sp = 0.0
    
    completed_total_sp = 0.0
    sprint_start_sp = 0.0  # Total Planned SP
    total_unplanned_sp = 0.0  # Total Unplanned SP (completed + incomplete) 
    
    sprint_id = sprint_info['id']
    sprint_name = sprint_info.get('name', '')
    
    debug_data = []

    for issue in issues:
        fields = issue['fields']
        
        # --- Python Sub-task Filter ---
        if fields['issuetype'].get('subtask', False):
            continue
            
        key = issue['key']
        issue_type = fields['issuetype']['name']
        
        # --- Status at Sprint End ---
        # If sprint is active (sprint_end is None), use current status (None passed to func).
        # If closed, reconstruct status at sprint_end.
        status_id_at_end = get_status_id_at_date(issue, sprint_end)
        
        # Determine strict completion based on Board Config + Time
        is_completed_for_stats = False
        completion_status_log = "Incomplete"

        if done_status_ids:
            if status_id_at_end in done_status_ids:
                is_completed_for_stats = True
                completion_status_log = "Completed"
            else:
                is_completed_for_stats = False
                completion_status_log = "Status not Done @ End"
        else:
            # Fallback Logic (simplified, assuming mostly covered by config)
             status_category = fields['status']['statusCategory']['key']
             if status_category == 'done':
                 res_date = parse_date(fields.get('resolutiondate'))
                 if res_date and sprint_end and res_date <= sprint_end:
                     is_completed_for_stats = True
                     completion_status_log = "Completed (Fallback)"
                 elif not sprint_end: # Active sprint, current status is done
                     is_completed_for_stats = True
                     completion_status_log = "Completed (Active)"
                 else:
                     is_completed_for_stats = False
                     completion_status_log = "Not Done (Fallback)"
             else:
                 is_completed_for_stats = False
                 completion_status_log = "Not Done (Fallback)"
        
        story_points = fields.get(sp_field_id)
        if story_points is None: 
            story_points = 0.0
        else:
            try:
                story_points = float(story_points)
            except:
                story_points = 0.0
            
        resolution_date_str = fields.get('resolutiondate')
        resolution_date = parse_date(resolution_date_str)
        
        changelog = issue.get('changelog', {}).get('histories', [])
        
        # --- Unplanned Logic ---
        is_unplanned = False
        created_date = parse_date(fields['created'])
        added_log = None
        
        if created_date and created_date > sprint_start:
            is_unplanned = True
            added_log = "Created after start"
        else:
            earliest_add = None
            for history in changelog:
                for item in history['items']:
                    if item['field'] == 'Sprint':
                        to_sprints_str = str(item.get('to', ''))
                        # Strip whitespace from each item after splitting
                        to_sprints_list = [s.strip() for s in to_sprints_str.split(',')]
                        
                        # Check: ID or Name (stripped)
                        if str(sprint_id) in to_sprints_list or sprint_name in to_sprints_list or sprint_name in to_sprints_str:
                            hist_date = parse_date(history['created'])
                            if earliest_add is None or hist_date < earliest_add:
                                earliest_add = hist_date
            
            if earliest_add and earliest_add > sprint_start:
                is_unplanned = True
                added_log = f"Added at {earliest_add}"

        # --- Metrics ---
        
        # "Completed Outside Sprint"
        # Logic: Entered sprint in a Done state?
        # Check status AT SPRINT START.
        status_id_at_start = get_status_id_at_date(issue, sprint_start)
        is_done_at_start = (status_id_at_start in done_status_ids) if done_status_ids else False
        
        is_completed_outside = False
        if is_done_at_start and is_completed_for_stats: 
            # If it started done AND ended done, it's completed outside/carried over done? 
            # Usually "Completed Outside" means "Done before sprint start".
            is_completed_outside = True
            completed_outside_count += 1
            completion_status_log = "Completed Outside"

        if is_completed_for_stats:
            completed_total_sp += story_points
            if is_unplanned:
                completed_unplanned += story_points
            else:
                completed_planned += story_points
        
        if not is_unplanned:
            sprint_start_sp += story_points
        else:
            total_unplanned_sp += story_points
            
        all_sprint_tasks_count += 1
        
        if not is_completed_for_stats:
            incomplete_count += 1
            
        if is_completed_for_stats and issue_type.lower() == 'bug' and not is_completed_outside:
            bugs_out_count += 1
            bugs_out_sp += story_points
            
        current_status_name = fields['status']['name']
        
        debug_data.append({
            "Key": key,
            "Type": issue_type,
            "Points": story_points,
            "Current Status": current_status_name,
            "Status ID @ End": status_id_at_end,
            "Stats Result": completion_status_log,
            "Is Unplanned": is_unplanned,
            "Reason": added_log or "Planned",
        })

    # Calculations
    velocity = completed_total_sp
    denom = all_sprint_tasks_count
    carryover_pct = (incomplete_count / denom * 100) if denom > 0 else 0.0
    # Fix: Planned Completion % should be (Completed Planned / Total Planned Scope [sprint_start_sp])
    planned_pct = (completed_planned / sprint_start_sp * 100) if sprint_start_sp > 0 else 0.0
    completion_pct_total = (completed_total_sp / final_capacity * 100) if final_capacity > 0 else 0.0
    
    metrics = {
        "velocity": velocity,
        "completed_planned": completed_planned,
        "completed_unplanned": completed_unplanned,
        "carryover_pct": carryover_pct,
        "bugs_in": len(bugs_in_issues),
        "bugs_out": bugs_out_count,
        "bugs_out_sp": bugs_out_sp,
        "completion_pct_total": completion_pct_total,
        "planned_pct": planned_pct,
        "unplanned_pct": (completed_unplanned / total_unplanned_sp * 100) if total_unplanned_sp > 0 else 0.0,
        "planned_sp": sprint_start_sp,
        "unplanned_sp": total_unplanned_sp,
        "task_count_completed": all_sprint_tasks_count - incomplete_count,
        "task_count_incomplete": incomplete_count,
        "task_count_total": all_sprint_tasks_count
    }
    return metrics, debug_data

def calculate_sprint_metrics_fast(domain, sprint_id, sprint_name, auth, sp_field_id, team_id, planned_cap, final_cap, done_status_ids):
    """
    Fast version of metrics calculation for trend loading.
    Skips debug data collection for better performance.
    Returns metrics dict or None on error.
    """
    try:
        sprint_info, issues = get_sprint_issues(domain, sprint_id, auth, sp_field_id)
        if not sprint_info:
            return None
        
        bugs_in_list = get_bugs_in(domain, sprint_info.get('completeDate'), team_id, auth)
        
        # Simplified calculation without debug data
        completed_planned = 0.0
        completed_unplanned = 0.0
        incomplete_count = 0
        all_sprint_tasks_count = 0
        bugs_out_count = 0
        bugs_out_sp = 0.0
        completed_total_sp = 0.0
        sprint_start_sp = 0.0
        total_unplanned_sp = 0.0
        
        sprint_start_str = sprint_info.get('startDate')
        sprint_end_str = sprint_info.get('endDate') or sprint_info.get('completeDate')
        sprint_start = parse_date(sprint_start_str)
        sprint_end = parse_date(sprint_end_str)
        
        for issue in issues:
            fields = issue['fields']
            issue_type = fields['issuetype']['name']
            
            # Skip sub-tasks
            if fields['issuetype'].get('subtask', False):
                continue
            
            # Determine completion status
            status_id_at_end = get_status_id_at_date(issue, sprint_end) if sprint_end else None
            is_completed = (status_id_at_end in done_status_ids) if done_status_ids and status_id_at_end else False
            
            story_points = fields.get(sp_field_id) or 0.0
            try:
                story_points = float(story_points)
            except:
                story_points = 0.0
            
            # Simple unplanned detection: created after sprint start
            created_date = parse_date(fields['created'])
            is_unplanned = created_date and sprint_start and created_date > sprint_start
            
            if is_completed:
                completed_total_sp += story_points
                if is_unplanned:
                    completed_unplanned += story_points
                else:
                    completed_planned += story_points
            
            if not is_unplanned:
                sprint_start_sp += story_points
            else:
                total_unplanned_sp += story_points
            
            all_sprint_tasks_count += 1
            if not is_completed:
                incomplete_count += 1
            if is_completed and issue_type.lower() == 'bug':
                bugs_out_count += 1
                bugs_out_sp += story_points
        
        velocity = completed_total_sp
        carryover_pct = (incomplete_count / all_sprint_tasks_count * 100) if all_sprint_tasks_count > 0 else 0.0
        planned_pct = (completed_planned / sprint_start_sp * 100) if sprint_start_sp > 0 else 0.0
        completion_pct_total = (completed_total_sp / final_cap * 100) if final_cap > 0 else 0.0
        
        return {
            "velocity": velocity,
            "completed_planned": completed_planned,
            "completed_unplanned": completed_unplanned,
            "carryover_pct": carryover_pct,
            "bugs_in": len(bugs_in_list) if bugs_in_list else 0,
            "bugs_out": bugs_out_count,
            "bugs_out_sp": bugs_out_sp,
            "completion_pct_total": completion_pct_total,
            "planned_pct": planned_pct,
            "unplanned_pct": (completed_unplanned / total_unplanned_sp * 100) if total_unplanned_sp > 0 else 0.0,
            "planned_sp": sprint_start_sp,
            "unplanned_sp": total_unplanned_sp,
            "task_count_completed": all_sprint_tasks_count - incomplete_count,
            "task_count_incomplete": incomplete_count,
            "task_count_total": all_sprint_tasks_count
        }
    except Exception as e:
        print(f"Error calculating metrics for sprint {sprint_id} ({sprint_name}): {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def load_trend_data(selected_sprint_id, sprints_list, domain, auth, sp_field_id, team_id, board_id, progress_callback=None):
    """
    Load metrics for selected sprint + 4 previous sprints.
    Uses cache-first strategy and parallel API calls.
    Returns DataFrame with all sprint metrics.
    """
    # Get existing metrics from DB
    df_existing = get_all_metrics()
    existing_ids = set(df_existing['sprint_id'].tolist()) if not df_existing.empty else set()
    
    # Pre-fetch done statuses once (Streamlit safe here)
    done_status_ids = get_board_done_statuses(domain, board_id, auth)
    
    # Find selected sprint index and get 5 sprints (selected + 4 previous)
    sprint_ids = [s['id'] for s in sprints_list]
    sprint_map = {s['id']: s for s in sprints_list}
    
    try:
        selected_idx = sprint_ids.index(selected_sprint_id)
    except ValueError:
        return df_existing
    
    # Get 5 sprints: selected + up to 4 previous
    target_ids = sprint_ids[selected_idx:min(selected_idx + 5, len(sprint_ids))]
    
    # Identify which sprints need fetching
    to_fetch = [(sid, sprint_map[sid]) for sid in target_ids if sid not in existing_ids]
    
    if progress_callback:
        progress_callback(f"Loading {len(target_ids)} sprints ({len(to_fetch)} need fetching)...")
    
    # Parallel fetch for missing sprints
    if to_fetch:
        def fetch_sprint(sprint_tuple):
            sid, sprint_info = sprint_tuple
            sprint_name = sprint_info.get('name', '')
            # Use default capacities (can be refined later)
            metrics = calculate_sprint_metrics_fast(domain, sid, sprint_name, auth, sp_field_id, team_id, 80, 80, done_status_ids)
            if metrics:
                save_metrics(sid, sprint_name, metrics)
            return sid, sprint_name, metrics
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(fetch_sprint, s): s for s in to_fetch}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if progress_callback:
                    progress_callback(f"Loaded {completed}/{len(to_fetch)} sprints...")
    
    # Return updated metrics
    return get_all_metrics()

# --- Streamlit UI ---
st.set_page_config(page_title="Jira Sprint Stats", layout="wide")

st.title("Jira Sprint Stats Automator")

# init DB
init_db()

# Load persisted configs
p_domain = get_config("domain", "")
p_email = get_config("email", "")
p_token = get_config("token", "")
p_board_id = get_config("board_id", "")
p_team_id = get_config("team_id", "5dd2e52a-43b1-4772-8344-279d946b391b")
p_sprint_limit = int(get_config("sprint_limit", "20"))

# Fixed Story Points field
sp_field_id = "customfield_10033"

with st.sidebar:
    st.header("Jira Connection")
    domain = st.text_input("Jira Domain", value=p_domain)
    email = st.text_input("Email", value=p_email)
    token = st.text_input("API Token", type="password", value=p_token)
    board_id = st.text_input("Board ID", value=p_board_id)
    team_id = st.text_input("Team ID (UUID)", value=p_team_id)
    
    # Webhook Config
    env_webhook = ""
    # Try to load from secrets if available
    try:
        env_webhook = st.secrets.get("WEBHOOK_URL", "")
    except:
        pass
        
    p_webhook = get_config("webhook_url", env_webhook)
    webhook_url = st.text_input("Webhook URL (Google Apps Script)", value=p_webhook, type="password")
    
    st.divider()
    st.header("Sprints")
    sprint_limit = st.number_input("Number of Sprints to Fetch", min_value=1, max_value=200, value=p_sprint_limit)
    
    if st.button("Fetch Sprints"):
        if domain and email and token and board_id:
            # Save configs
            save_config("domain", domain)
            save_config("email", email)
            save_config("token", token)
            save_config("board_id", board_id)
            save_config("team_id", team_id)
            save_config("webhook_url", webhook_url)
            save_config("sprint_limit", sprint_limit)
            
            auth = get_auth_header(email, token)
            sprints = get_sprints(domain, board_id, auth, limit=sprint_limit)
            if sprints:
                st.session_state['sprints_map'] = {s['name']: s['id'] for s in sprints}
                st.session_state['sprints_list'] = sprints  # Store full list for trend loading
                st.session_state['board_id'] = board_id  # Store for trend loading
                st.success(f"Fetched {len(sprints)} sprints")
            else:
                st.error("No sprints found or error.")
        else:
            st.warning("Please fill all connection details.")

if 'sprints_map' in st.session_state:
    sprint_names = list(st.session_state['sprints_map'].keys())
    selected_sprint_name = st.selectbox("Select Sprint", sprint_names)
    selected_sprint_id = st.session_state['sprints_map'][selected_sprint_name]
    
    # Capacity Inputs
    db_planned, db_final = get_capacity(selected_sprint_id)
    
    col_cap1, col_cap2 = st.columns(2)
    with col_cap1:
        planned_cap = st.number_input("Planned Capacity", value=float(db_planned))
    with col_cap2:
        final_cap = st.number_input("Final Capacity", value=float(db_final))
        
    save_capacity(selected_sprint_id, selected_sprint_name, planned_cap, final_cap)
    
    if st.button("Fetch & Calculate Metrics"):
        with st.spinner("Fetching and calculating..."):
            auth = get_auth_header(email, token)
            done_status_ids = get_board_done_statuses(domain, board_id, auth)
            sprint_info, issues = get_sprint_issues(domain, selected_sprint_id, auth, sp_field_id)
            bugs_in_list = get_bugs_in(domain, sprint_info.get('completeDate'), team_id, auth)
            
            metrics, debug_list = calculate_stats(sprint_info, issues, bugs_in_list, planned_cap, final_cap, sp_field_id, done_status_ids)
            
            # Store breakdown in session_state so it persists across reruns
            st.session_state['last_breakdown'] = debug_list
            st.session_state['last_sprint_id'] = selected_sprint_id
            
            save_metrics(selected_sprint_id, selected_sprint_name, metrics)
            st.success("Metrics updated!")

    if webhook_url:
        st.write("### Export")
        try:
            df_all_ex = get_all_metrics()
            if not df_all_ex.empty:
                row_ex = df_all_ex[df_all_ex['sprint_id'] == selected_sprint_id]
                if not row_ex.empty:
                    met_ex = row_ex.iloc[0]
                    
                    payload = {
                        "sprintName": met_ex.get('sprint_name', selected_sprint_name),
                        "velocity": met_ex['velocity'],
                        "completedPlanned": met_ex['completed_planned'],
                        "completedUnplanned": met_ex['completed_unplanned'],
                        "completedTasks": int(met_ex.get('task_count_completed', 0)), 
                        "incompleteTasks": int(met_ex.get('task_count_incomplete', 0)),
                        "carryover": met_ex['carryover_pct'],
                        "plannedPct": met_ex['planned_pct'],
                        "plannedCompletionPct": met_ex['planned_pct'],
                        "unplannedCompletionPct": met_ex.get('unplanned_pct', 0),
                        "totalCompletionPct": met_ex['completion_pct_total'],
                        "taskCompletionPct": 100 - met_ex['carryover_pct'],
                        "bugsIn": int(met_ex['bugs_in']),
                        "bugsOut": int(met_ex['bugs_out']),
                        "plannedSP": met_ex.get('planned_sp', 0.0),
                        "unplannedSP": met_ex.get('unplanned_sp', 0.0)
                    }
                    
                    json_str = json.dumps(payload)
                    params = urllib.parse.quote(json_str)
                    export_link = f"{webhook_url}?data={params}"
                    
                    st.link_button("Export to Google Sheets", export_link)
                else:
                    st.warning("Calculate metrics to enable export.")
        except Exception as e:
            st.error(f"Export prep failed: {e}")

    # Display Metrics
    df_all = get_all_metrics()
    current_metrics = df_all[df_all['sprint_id'] == selected_sprint_id]
    
    if not current_metrics.empty:
        met = current_metrics.iloc[0]
        st.subheader(f"Stats for {selected_sprint_name}")
        
        # Calculate derived metrics
        planned_sp = met['completed_planned'] + met['completed_unplanned']  # This is velocity basically
        planned_pct_capacity = (planned_sp / planned_cap * 100) if planned_cap > 0 else 0.0
        task_completion_pct = 100 - met['carryover_pct']
        
        m_c1, m_c2, m_c3, m_c4 = st.columns(4)
        m_c1.metric("Velocity", f"{met['velocity']:.1f}")
        m_c2.metric("Task Completion %", f"{task_completion_pct:.1f}%")
        m_c3.metric("Planned Completion %", f"{met['planned_pct']:.1f}%")
        m_c4.metric("Total Completion %", f"{met['completion_pct_total']:.1f}%")
        
        m_c5, m_c6, m_c7, m_c8 = st.columns(4)
        m_c5.metric("Completed Planned", f"{met['completed_planned']:.1f}")
        m_c6.metric("Completed Unplanned", f"{met['completed_unplanned']:.1f}")
        m_c7.metric("Bugs In", int(met['bugs_in']))
        m_c8.metric("Bugs Out", f"{int(met['bugs_out'])} ({met.get('bugs_out_sp', 0.0):.1f} SP)")
        
        m_c9, m_c10, m_c11, m_c12 = st.columns(4)
        m_c9.metric("Planned %", f"{planned_pct_capacity:.1f}%")
        m_c10.metric("Carryover %", f"{met['carryover_pct']:.1f}%")
        # Leave placeholders or remove if not needed
        m_c11.metric("Unplanned Completion %", f"{met.get('unplanned_pct', 0.0):.1f}%")
        m_c12.empty()

    # Show persisted breakdown if available for current sprint
    if st.session_state.get('last_sprint_id') == selected_sprint_id and st.session_state.get('last_breakdown'):
        with st.expander("Show Detailed Issue Breakdown", expanded=False):
            df_breakdown = pd.DataFrame(st.session_state['last_breakdown'])
            if "Status ID @ End" in df_breakdown.columns:
                df_breakdown = df_breakdown.drop(columns=["Status ID @ End"])
            st.dataframe(df_breakdown, use_container_width=True)

    st.divider()
    st.subheader("📊 Sprint Insights")
    
    if not df_all.empty and not current_metrics.empty:
        met = current_metrics.iloc[0]
        
        # Use actual planned_sp and unplanned_sp from metrics (with fallback for old data)
        planned_sp_total = met.get('planned_sp', planned_cap) if 'planned_sp' in met and met['planned_sp'] > 0 else planned_cap
        unplanned_sp_total = met.get('unplanned_sp', 0) if 'unplanned_sp' in met else 0
        
        # --- Chart 1 & 2: Planned vs Completed (side by side) ---
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("##### Planned SP vs Completed Planned")
            fig1 = go.Figure(data=[
                go.Bar(name='Planned SP', x=['Planned SP'], y=[planned_sp_total], 
                       marker_color='#4285F4', text=[f"{planned_sp_total:.1f}"], textposition='inside', textfont=dict(color='white', size=16)),
                go.Bar(name='Completed Planned SP', x=['Completed Planned SP'], y=[met['completed_planned']], 
                       marker_color='#EA4335', text=[f"{met['completed_planned']:.1f}"], textposition='inside', textfont=dict(color='white', size=16))
            ])
            fig1.update_layout(barmode='group', height=300, margin=dict(l=20, r=20, t=30, b=20), showlegend=True,
                               legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5))
            fig1.update_yaxes(range=[0, max(planned_sp_total, met['completed_planned']) * 1.2])
            st.plotly_chart(fig1, use_container_width=True)
        
        with chart_col2:
            st.markdown("##### Unplanned SP vs Completed Unplanned")
            fig2 = go.Figure(data=[
                go.Bar(name='Unplanned SP', x=['Unplanned SP'], y=[unplanned_sp_total], 
                       marker_color='#4285F4', text=[f"{unplanned_sp_total:.1f}"], textposition='inside', textfont=dict(color='white', size=16)),
                go.Bar(name='Completed Unplanned SP', x=['Completed Unplanned SP'], y=[met['completed_unplanned']], 
                       marker_color='#EA4335', text=[f"{met['completed_unplanned']:.1f}"], textposition='inside', textfont=dict(color='white', size=16))
            ])
            fig2.update_layout(barmode='group', height=300, margin=dict(l=20, r=20, t=30, b=20), showlegend=True,
                               legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5))
            fig2.update_yaxes(range=[0, max(unplanned_sp_total, met['completed_unplanned'], 1) * 1.2])
            st.plotly_chart(fig2, use_container_width=True)
        
        # --- Chart 3 & 4: Pie Chart + Bugs ---
        chart_col3, chart_col4 = st.columns(2)
        
        with chart_col3:
            st.markdown("##### SP Breakdown (Planned vs Unplanned)")
            total_completed = met['completed_planned'] + met['completed_unplanned']
            if total_completed > 0:
                fig3 = go.Figure(data=[go.Pie(
                    labels=['Planned', 'Unplanned'],
                    values=[met['completed_planned'], met['completed_unplanned']],
                    marker=dict(colors=['#4285F4', '#EA4335']),
                    textinfo='label+percent',
                    textposition='outside',
                    textfont=dict(size=14),
                    hole=0
                )])
                fig3.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20), showlegend=False)
                st.plotly_chart(fig3, use_container_width=True)
            else:
                st.info("No completed SP to display.")
        
        with chart_col4:
            st.markdown("##### Bugs In vs Bugs Out")
            fig4 = go.Figure(data=[
                go.Bar(name='Bugs In', x=['Bugs In'], y=[met['bugs_in']], 
                       marker_color='#4285F4', text=[int(met['bugs_in'])], textposition='inside', textfont=dict(color='white', size=16)),
                go.Bar(name='Bugs Out', x=['Bugs Out'], y=[met['bugs_out']], 
                       marker_color='#EA4335', text=[int(met['bugs_out'])], textposition='inside', textfont=dict(color='white', size=16))
            ])
            fig4.update_layout(barmode='group', height=300, margin=dict(l=20, r=20, t=30, b=20), showlegend=True,
                               legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5))
            st.plotly_chart(fig4, use_container_width=True)
        
        # --- Trend Charts (last 5 sprints) - Auto-load missing data ---
        st.divider()
        st.markdown("#### 📈 5-Sprint Trends")
        
        target_ids = []
        
        # Auto-load trend data if sprints are available
        if 'sprints_list' in st.session_state and st.session_state['sprints_list']:
            sprints_list = st.session_state['sprints_list']
            
            # Check if we need to load more data
            df_check = get_all_metrics()
            existing_ids = set(df_check['sprint_id'].tolist()) if not df_check.empty else set()
            
            # Get target sprint IDs based on selected sprint position
            sprint_ids_list = [s['id'] for s in sprints_list]
            try:
                selected_idx = sprint_ids_list.index(selected_sprint_id)
                # Get selected sprint + 4 OLDER sprints (they come after in the list since list is newest-first)
                target_ids = sprint_ids_list[selected_idx:min(selected_idx + 5, len(sprint_ids_list))]
                missing_ids = [sid for sid in target_ids if sid not in existing_ids]
                missing_count = len(missing_ids)
            except ValueError:
                target_ids = []
                missing_ids = []
                missing_count = 0
            
            if missing_count > 0:
                try:
                    with st.spinner(f"Loading trend data..."):
                        auth = get_auth_header(email, token)
                        df_all = load_trend_data(
                            selected_sprint_id, 
                            sprints_list, 
                            domain, 
                            auth, 
                            sp_field_id, 
                            team_id,
                            st.session_state['board_id']
                        )
                except Exception as e:
                    st.error(f"Error auto-loading trend data: {str(e)}")
                    df_all = df_check
            else:
                df_all = df_check
            
            # Filter to show only the 5 relevant sprints for trends (target_ids)
            if target_ids:
                df_sorted = df_all[df_all['sprint_id'].isin(target_ids)].sort_values('sprint_id', ascending=True)
            else:
                df_sorted = pd.DataFrame()
        else:
            # Fallback if no sprints_list available
            df_sorted = df_all.sort_values('sprint_id', ascending=False).head(5).iloc[::-1]
        
        if len(df_sorted) >= 2:
            
            # Use sprint names from database if available, otherwise fallback to ID
            sprint_labels = []
            for idx, row in df_sorted.iterrows():
                name = row.get('sprint_name', None)
                if name and isinstance(name, str):
                    # Extract short name like "IR19" from "Artisans Iteration 19 2025"
                    if 'Iteration' in name:
                        parts = name.split()
                        for i, p in enumerate(parts):
                            if p == 'Iteration' and i + 1 < len(parts):
                                sprint_labels.append(f"IR{parts[i+1]}")
                                break
                        else:
                            sprint_labels.append(name[:15])
                    else:
                        sprint_labels.append(name[:15])
                else:
                    sprint_labels.append(f"Sprint {int(row['sprint_id'])}")
            
            trend_col1, trend_col2 = st.columns(2)
            
            with trend_col1:
                st.markdown("##### Task & SP Completion %")
                task_comp = (100 - df_sorted['carryover_pct']).tolist()
                sp_comp = df_sorted['completion_pct_total'].tolist()
                
                fig5 = go.Figure()
                fig5.add_trace(go.Scatter(
                    x=sprint_labels, y=task_comp,
                    mode='lines+markers+text', name='Task Completion %', 
                    line=dict(color='#4285F4', width=2),
                    text=[f"{v:.0f}%" for v in task_comp], textposition='top center', textfont=dict(color='#4285F4', size=11)
                ))
                fig5.add_trace(go.Scatter(
                    x=sprint_labels, y=sp_comp,
                    mode='lines+markers+text', name='SP Completion %', 
                    line=dict(color='#F4A235', width=2),
                    text=[f"{v:.0f}%" for v in sp_comp], textposition='bottom center', textfont=dict(color='#F4A235', size=11)
                ))
                fig5.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=40),
                                   legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
                                   yaxis=dict(range=[0, 110]))
                st.plotly_chart(fig5, use_container_width=True)
            
            with trend_col2:
                st.markdown("##### Planned & Completion Metrics")
                planned_pct_vals = df_sorted['planned_pct'].tolist()
                comp_total_vals = df_sorted['completion_pct_total'].tolist()
                # Calculate unplanned completion % if we have the data
                unplanned_comp_vals = []
                for idx, row in df_sorted.iterrows():
                    unplanned_sp = row.get('unplanned_sp', 0)
                    if unplanned_sp and unplanned_sp > 0:
                        unplanned_comp_vals.append((row['completed_unplanned'] / unplanned_sp) * 100)
                    else:
                        unplanned_comp_vals.append(0)
                
                fig6 = go.Figure()
                fig6.add_trace(go.Scatter(
                    x=sprint_labels, y=planned_pct_vals,
                    mode='lines+markers+text', name='% Planned', 
                    line=dict(color='#4285F4', width=2),
                    text=[f"{v:.0f}%" for v in planned_pct_vals], textposition='top center', textfont=dict(color='#4285F4', size=10)
                ))
                fig6.add_trace(go.Scatter(
                    x=sprint_labels, y=planned_pct_vals,
                    mode='lines+markers+text', name='% Completion (planned)', 
                    line=dict(color='#EA4335', width=2),
                    text=[f"{v:.0f}%" for v in planned_pct_vals], textposition='bottom center', textfont=dict(color='#EA4335', size=10)
                ))
                fig6.add_trace(go.Scatter(
                    x=sprint_labels, y=comp_total_vals,
                    mode='lines+markers+text', name='% Completion (total)', 
                    line=dict(color='#FBBC04', width=2),
                    text=[f"{v:.0f}%" for v in comp_total_vals], textposition='bottom center', textfont=dict(color='#FBBC04', size=10)
                ))
                if any(v > 0 for v in unplanned_comp_vals):
                    fig6.add_trace(go.Scatter(
                        x=sprint_labels, y=unplanned_comp_vals,
                        mode='lines+markers+text', name='% Completion (unplanned)', 
                        line=dict(color='#9E9E9E', width=2),
                        text=[f"{v:.0f}%" for v in unplanned_comp_vals], textposition='top center', textfont=dict(color='#9E9E9E', size=10)
                    ))
                fig6.update_layout(height=350, margin=dict(l=20, r=20, t=50, b=40),
                                   legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
                                   yaxis=dict(range=[0, 110]))
                st.plotly_chart(fig6, use_container_width=True)
        else:
            st.info("Need at least 2 sprints of data for trend charts.")
    else:
        st.info("No history data yet. Calculate some sprints to see charts!")

else:
    st.info("Please fetch sprints to begin.")

