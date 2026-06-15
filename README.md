# TaskFlow — Engineering Operations Platform

  A complete engineering operations platform built with zero external dependencies.
  Single HTML file frontend. Python stdlib backend. SQLite database. Runs anywhere.

  ## Modules

  ### Project & Task Management
  - **Dashboard** — live project health overview across all active programmes
  - **Tasks** — full task register with assignment, priority, status, and deadline tracking
  - **Kanban** — drag-and-drop board view per project
  - **Gantt Chart** — admin timeline view with assignee, hours, and milestone tracking
  - **Calendar** — cross-project schedule view

  ### Team & Collaboration
  - **Chat** — team messaging per project
  - **Timesheets** — individual time logging; auto-generates project-wise reports
  - **Issues** — issue and escalation tracking with resolution workflow
  - **Performance** — team performance dashboard per member
  - **Members** — role-based access control; active / archived member management
  - **Notifications** — system-wide alert and acknowledgement tracking

  ### Engineering & Design
  - **BOM** — bill of materials management with release workflow
  - **Standard BOM** — reusable standard component library linked to stocked inventory
  - **Change Notes (ECN)** — engineering change request and approval tracking
  - **OEM** — OEM component and variant management
  - **Indent** — material indent request workflow
  - **Approvals** — multi-level approval queue with audit logging
  - **Validation** — design validation and sign-off tracking

  ### Supply Chain & Inventory
  - **Material Inward** — goods receipt and inward inspection logging
  - **Issue Slip** — stock issue tracking against BOM and project
  - **Inventory** — live stock levels and movement history
  - **Supplier Master** — vendor database with contact and performance records
  - **Purchase Orders** — PO creation, tracking, and closure

    ### Reporting & Admin
  - **Reports** — cross-module reporting and export
  - **Audit Log** — full action history with timestamps and user attribution
  - **Settings** — organisation configuration and system preferences

  ## Stack

  | Layer | Technology |
  |-------|-----------|
  | Frontend | HTML / CSS / JavaScript — zero frameworks |
  | Backend | Python `http.server` + `sqlite3` — stdlib only |
  | Database | SQLite (single file, zero setup) |
  | Dependencies | **None** — no npm, no pip, no Docker |

  Runs on any machine with Python 3.x installed.

  ## Run Locally

  bash
  git clone https://github.com/kumarmechatronics/engineering-workflow-platform.git
  cd engineering-workflow-platform
  python server.py

  Open http://localhost:8000 in your browser.

  For a UI preview without data persistence, open taskflow_app.html directly in any browser.

  Background

  Built and deployed in active production use at Effica Automation, Coimbatore — replacing paper-based
  processes across design, procurement, HR, and project management for a mechanical engineering team.

  Author

  Kumar Thirumavalavan — Design Manager & Lead Mechanical Engineer
  https://linkedin.com/in/kumarthirumavalavan
