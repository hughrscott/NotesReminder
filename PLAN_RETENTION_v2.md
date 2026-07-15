# Retention Intelligence Plan — v2
> Written: July 15, 2026 | For: Hugh Scott | Saved: `PLAN_RETENTION_v2.md`

## Premise

We have more data than we're using. The current v11 model is correct (6/6 signs, AUC 0.96) but it only answers "who might leave?" — it doesn't answer "why?" or "what should we do about it?" This plan proposes a shift from prediction to intelligence: using every data source to understand each student's situation and recommend specific, personalized interventions.

---

## Part 1: At-Risk Student Intelligence

### 1.1 Churn Archetypes (Pattern Recognition)

Instead of a single risk score, classify each at-risk student into one of several churn patterns. Each pattern has a recommended intervention playbook.

| Archetype | Signals | Intervention |
|---|---|---|
| **Sudden Stop** | Was attending regularly, then abruptly stopped. Notes were positive. No cancel phrases in comms. | Life event. Reach out with flexibility — offer to hold their spot, different time, or pause plan. Don't push. |
| **Gradual Fade** | Attendance declining over 8+ weeks. Notes becoming shorter or lower quality. Parent comms less frequent. | Losing interest. Re-engage with new goals: suggest a performance, camp, or different instructor/style. |
| **Schedule Conflict** | SMS/voicemails mention "busy," "schedule," "can't make." Irregular lesson gaps. | Offer alternative time slots, biweekly option, or online lessons. Don't lose them — adapt. |
| **Quality Fade** | Note scores declining (5→3→1). Instructor mentions frustration, lack of practice, plateau. | Student frustrated with progress. Consider instructor change, different approach, or temporary break with return date. |
| **Financial Stress** | Plan changes, payment mentions in comms, hold requests. | Offer lower-tier plan, scholarship info, or payment plan. Better to keep them at lower revenue than lose them. |
| **Communication Red Flags** | Cancel/dissatisfaction phrases in parent comms. Voicemail sentiment negative. Multiple call attempts. | Parent is frustrated. Direct GM call — don't delegate to instructor. Acknowledge issue, offer solution. |
| **New Student Risk** | <90 days tenure, <5 lessons. No parent contact on file. | Standard onboarding gap. Welcome call, confirm goals, schedule first performance. |

### 1.2 Multi-Source Signal Synthesis

For EACH at-risk student, compile a "retention brief" from all available data:

**Lesson Intelligence:**
- What songs/pieces were they working on? (from note text)
- What were their biggest challenges? (from note text — "struggling with timing," "needs more practice")
- What were their breakthroughs? ("nailed the solo," "finally got the rhythm")
- Score trajectory over time (chart)
- No-show pattern (dates, frequency)
- Instructor changes (who, when)

**Communication Intelligence:**
- Parent contact info (phone, email, preferred channel)
- Last parent contact date and content
- Tone over time (sentiment trajectory)
- Specific concerns raised ("he's bored," "too expensive," "schedule doesn't work")
- Unanswered questions or unresolved issues

**Pike13 Intelligence:**
- Plan type and price
- Current hold status and dates
- Account manager details (who pays, contact info)
- Billing status (auto-bill active? invoices past due?)
- Dependents (siblings also enrolled? — family churn risk)

**Attendance Intelligence:**
- Lesson frequency pattern (weekly, biweekly, irregular)
- Gap analysis: when did they start missing?
- Comparison to their own 12-month baseline (not seasonal)
- Makeup credit accumulation vs burn rate

### 1.3 Intervention Recommendation Engine

For each archetype, generate a specific, actionable recommendation:

**Format:**
```
STUDENT: [Name]
ARCHETYPE: [Sudden Stop / Gradual Fade / etc.]
CONFIDENCE: [High/Medium/Low — based on data completeness]
LAST CONTACT: [Date, channel, summary]

WHAT TO SAY:
"[Draft opening referencing their specific progress or situation]"

WHY THIS APPROACH:
"[Rationale tied to their specific data]"

CONTACT INFO:
Parent: [name, phone, email]
Best time: [from Pike13 schedule patterns]

DEADLINE: [If hold ending / season starting / etc.]
```

### 1.4 Hold Status Integrity

**Rule: Never flag a student for non-attendance if their non-attendance is explained by a hold.**

Before generating any at-risk alert, check Pike13 hold data. If the student is on hold:
- Do NOT include them in "stopped attending" counts
- Do NOT recommend "reach out — they haven't been seen"
- Instead: note when their hold ends and move them to "Returning" section

This is already partially implemented. The plan is to make it a gate at the data ingestion level, not a post-hoc filter.

---

## Part 2: Returning Student Re-Engagement

### 2.1 The Opportunity Window

Students returning from hold are at a decision point. They paused their membership — they could easily decide not to come back. The 2 weeks before and after hold-end are the critical re-engagement window.

### 2.2 Re-Engagement Intelligence

For each returning student, compile:

**Progress Continuity:**
- What were they last working on? (specific songs/pieces from notes)
- What was their last achievement? (highest score, performance, milestone)
- What's the natural next step? (next song in sequence, next performance season, next camp)

**Social Connection:**
- Who was their instructor? (same one available?)
- Were they in a band/rehearsal group? (groupmates' names)
- Any siblings also enrolled? (family re-engagement)

**Excitement Triggers:**
- Upcoming performances or camps they could join
- New songs/genres that match their taste (from note text)
- Milestone they were approaching (first show, 100th lesson, etc.)
- Instructor's specific praise from last notes ("you're really getting this")

**Schedule Intelligence:**
- Their preferred day/time (from historical lesson patterns)
- Is that slot still available?
- Any conflicts we know about (from communication history)

### 2.3 Re-Engagement Outreach Templates

Three levels based on data richness:

**Level 1 — Rich Data (notes + comms + history):**
```
Hi [parent name],

I noticed [student]'s hold ends [date] and wanted to reach out before then. 
[Instructor] was really impressed with [specific achievement from notes] 
before the break — [he/she] said [quote from note].

We've got [upcoming event/performance/camp] coming up on [date] that I 
think [student] would love. [His/Her] old [day/time] slot is still 
available if that works.

Would you like me to hold it?
```

**Level 2 — Moderate Data (some notes, limited comms):**
```
Hi [parent name],

[Student]'s hold wraps up [date] — we'd love to have [him/her] back! 
[Instructor] had [him/her] working on [topic/genre] and making great 
progress.

What time works best these days? Happy to find a slot that fits.
```

**Level 3 — Sparse Data (no notes, no comms):**
```
Hi [parent name],

Just checking in as [student]'s hold ends [date]. Hope the break was 
great! We'd love to welcome [him/her] back — what's the best way to 
get [him/her] re-scheduled?
```

### 2.4 Hold-End Timeline Actions

| Timing | Action |
|---|---|
| **14 days before** | Send re-engagement email with personal hook |
| **7 days before** | If no response, follow up via preferred channel (SMS if parent texts, call if they call) |
| **Hold end date** | Check if they've been re-scheduled. If not, one more outreach. |
| **7 days after** | If still no response, flag for GM direct call. |

---

## Part 3: Data Sources We Should Be Mining

### Sources we have but aren't fully using:

| Source | What We Use Now | What We Should Use |
|---|---|---|
| Lesson note TEXT | Scores only | Song names, challenges, breakthroughs, instructor tone, student preferences |
| SMS message content | Cancel phrase counts only | Schedule discussions, tone, specific questions, parent personality |
| Voicemail transcripts | Sentiment score only | Specific concerns raised, emotional content, urgency level |
| Call review transcripts | Not used at all | Staff notes on parent conversations, action items, promises made |
| School emails | Not used at all | Parent-school correspondence, complaints, praise, questions |
| Pike13 plan details | Hold dates only | Price, commitment, auto-bill, invoice status, plan changes |
| Instructor-student relationships | Not used | Which instructors retain best? Which students requested changes? |
| Band/rehearsal groups | Not used | Social connection — if one bandmate leaves, others at risk? |
| Performance history | Not used | Do students who perform stay longer? Is there a "first show" retention effect? |

---

## Part 4: Implementation Roadmap

### Phase A: Pattern Classification (build first)
- Implement churn archetype detection using multi-source signals
- Replace single risk score with archetype + confidence
- Each archetype has a recommended intervention playbook

### Phase B: Retention Brief Generator
- For every flagged student, auto-generate a one-page brief
- Pulls from ALL data sources (notes, comms, Pike13, attendance)
- Includes: draft outreach text, contact info, specific hooks

### Phase C: Returning Student Engine  
- Auto-detect students approaching hold-end (14, 7, 0 days)
- Generate re-engagement messages with personal hooks
- Track response rates (did they come back?)

### Phase D: Outcome Tracking
- Track which interventions were tried and whether the student stayed
- Learn which approaches work for which archetypes
- Feed back into recommendation quality over time

---

## What We Should NOT Do

1. **Don't rush to implement.** The current v11 system is working correctly. Build the intelligence layer on top of it, don't rebuild it.
2. **Don't over-automate outreach.** The emails I sent you are YOUR decision support, not auto-send to parents. You're the GM — the data should inform your judgment, not replace it.
3. **Don't chase AUC.** We already proved that high AUC = wrong coefficients. Focus on actionability, not metrics.
4. **Don't flag on-hold students as at-risk.** Already partially fixed, but make it a hard gate, not a filter.

---

*This plan saved as `PLAN_RETENTION_v2.md` in the project root. Ready to continue tomorrow.*
