# 🚀 Matrix Cloud Relay Multi-Userbot Worker (Template for 12 Repositories)

This repository contains the lightweight multi-userbot worker deployed across your **12 public GitHub worker repositories**.
It connects to your central database on Render, downloads media from restricted Telegram groups, and uploads it to target chats using GitHub's **10Gbps unmetered Azure network** (preserving 100% of Render's bandwidth).

---

## ⚡ Deployment Instructions (One-Time Setup)

### Step 1: Push this template to GitHub
Create your first repository in your GitHub Organization (e.g. `relay-worker-01`) as a **Public** repository and push the contents of this folder:
```bash
git init
git add .
git commit -m "Init multi-userbot relay worker"
git branch -M main
git remote add origin https://github.com/<YOUR_ORGANIZATION>/relay-worker-01.git
git push -u origin main
```

### Step 2: Configure Secrets (Only 3 Secrets Needed!)
In your GitHub Organization: Go to **Settings** -> **Secrets and variables** -> **Actions** -> **New organization secret**:

| Secret Name | Description | Example |
| :--- | :--- | :--- |
| `TG_API_ID` | Telegram API ID | `37516521` |
| `TG_API_HASH` | Telegram API Hash | `53ac5c8bf864f91c4984c5ba7d1686b2` |
| `DATABASE_URL` | External PostgreSQL Database URL from Render | `postgresql://user:pass@dpg-xxx.render.com/dbname?sslmode=require` |

> 🔒 **Privacy & Anti-Leak Architecture:** 
> * **Zero Userbot Sessions in GitHub:** Userbot session strings are **NOT** stored in GitHub Secrets or anywhere in this repository!
> * When a restricted message arrives on Render, the Master Bot dynamically attaches the session string of whichever userbot is a member of that group inside your secure PostgreSQL database queue.
> * The runner pulls the task over TLS/SSL directly into RAM, connects dynamically, transfers the media, and cleans up immediately.
> * Logs in GitHub Actions are strictly sanitized and **never** print session strings, auth keys, chat IDs, or message text.

---

### Step 3: Duplicate into 12 Repositories (Takes 2 minutes)
1. Go to `relay-worker-01` -> **Settings** -> check **Template repository**.
2. Click the green **"Use this template"** button to create:
   - `relay-worker-02`
   - `relay-worker-03`
   - `...`
   - `relay-worker-12`

*(Because secrets are configured at the Organization level, all 12 repos inherit them automatically!)*

---

### Step 4: Add the 12 Repos to your Bot Environment on Render
In your Master Bot environment variables on Render, add:
```env
GITHUB_TOKEN=ghp_yourPersonalAccessTokenHere
CLOUD_RELAY_REPOS=myorg/relay-worker-01,myorg/relay-worker-02,myorg/relay-worker-03,myorg/relay-worker-04,myorg/relay-worker-05,myorg/relay-worker-06,myorg/relay-worker-07,myorg/relay-worker-08,myorg/relay-worker-09,myorg/relay-worker-10,myorg/relay-worker-11,myorg/relay-worker-12
```

---

## 🛡️ Reliability Guarantees
* **Multi-Userbot Support:** Dynamically switches between all 4–5 userbots on the fly depending on which userbot has access to the restricted source chat.
* **Bypasses 6-Hour Timeout:** The runner cleanly exits at **5 hours** (before GitHub's 6-hour hard cutoff).
* **Bypasses Abuse Scanners:** Because shifts rotate through 12 repositories, each repository only runs once every **60 hours**.
* **15-Min Auto-Sleep:** If groups are quiet and the queue stays empty for 15 minutes, the runner automatically shuts down to save compute.
