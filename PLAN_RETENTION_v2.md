# Retention Intelligence Plan — v2 (Final)
> Written: July 15, 2026 | Reviewed: Gemini 3.5 | For: Hugh Scott  
> Saved: `PLAN_RETENTION_v2.md` | Status: Ready for Hugh's review | Cron: **PAUSED**

---

## The Shift

We have a working prediction model (v11: 6/6 correct signs, AUC 0.96). It answers "who might leave?" — correctly. But it doesn't answer "why?" or "what should we do about it?"

This plan proposes building a **retention intelligence layer** on top of v11: using ALL available data to understand each student's situation and recommend specific, personalized interventions. The model says *who*. The intelligence layer says *what to do*.

---

## Part 1: At-Risk Student Intelligence

### 1.1 Seven Churn Archetypes (Pattern Recognition)

Each at-risk student gets classified into a churn pattern. Each pattern has a recommended intervention playbook. Patterns can evolve as new data reveals new behaviors.

| # | Archetype | Signals | Intervention |
|---|---|---|---|
| 1 | **Disengagement** | Attendance declining (any speed). Notes getting shorter or less detailed. Parent comms tapering off. Merges the old "Sudden Stop" and "Gradual Fade" — same cause, different speed. Speed indicator included ("rapid" vs "gradual"). | Re-engage with new goals. Reference their best recent progress. Offer a performance, camp, or fresh challenge. For rapid disengagement, check for life events first. |
| 2 | **Schedule Conflict** | SMS/voicemails mention "busy," "schedule," "can't make." Irregular lesson gaps. Multiple reschedule attempts. | Offer alternative time slots, biweekly option, or online lessons. Adapt — don't lose them over logistics. |
| 3 | **Quality Fade** | Note scores declining (5→3→1). Instructor mentions "frustrated," "struggling," "not practicing." Student hitting a plateau. | Consider instructor change, different teaching approach, or temporary break with a return date. Sometimes the chemistry needs a reset. |
| 4 | **Instructor Relationship** | Recent instructor change preceded attendance drop. Notes from one instructor consistently lower than others for the same student. Student thrived with instructor A but stalled with instructor B. | Review instructor-student fit. Did they do better with a different teacher? Consider switching back or trying someone new. |
| 5 | **Financial Stress** | Plan changes. Payment mentions in comms. Hold requests. Invoice past due. Moving to lower-tier plan. | Offer lower-tier plan, scholarship info, or payment plan. Better to keep them at reduced revenue than lose them entirely. |
| 6 | **Communication Red Flags** | Cancel/dissatisfaction phrases in parent comms. Voicemail sentiment negative. Multiple call attempts without resolution. Parent sounds frustrated. | Direct GM call. Don't delegate to instructor. Acknowledge the issue, offer a solution. This is the highest-priority archetype — angry parents leave and tell others. |
| 7 | **New Student Risk** | <90 days tenure, <5 lessons. No parent contact on file. No notes written yet. | Standard onboarding gap. Welcome call, confirm goals, schedule first performance. Simple but essential. |

### 1.2 Multi-Source Signal Synthesis

For each at-risk student, compile a retention brief from ALL available data:

**Lesson Intelligence:**
- Songs/pieces they were working on (from note text, not just scores)
- Challenges and breakthroughs (from note text — "struggling with timing," "nailed the solo")
- Score trajectory charted over time
- No-show pattern (dates, frequency, any explanation in comms)
- Instructor changes and their effects

**Communication Intelligence:**
- Parent contact info and preferred channel
- Tone trajectory (sentiment over time — does it darken before churn?)
- Specific concerns raised ("he's bored," "too expensive," "schedule conflict")
- Unanswered questions or unresolved issues
- Last positive interaction (use as an opener)

**Pike13 Intelligence:**
- Plan type, price, commitment
- Hold status and dates (checked BEFORE any risk flag — hard gate)
- Account manager details
- Billing status (auto-bill active? invoices past due?)
- Siblings also enrolled? (family churn risk)

**Attendance Intelligence:**
- Frequency pattern and changes over time
- When did the disruption start?
- Comparison to their OWN 12-month baseline (not seasonal)
- Makeup credit accumulation rate

### 1.3 Intervention Format

Every at-risk student gets:

```
STUDENT: [Name]
ARCHETYPE: [Pattern + Speed/Degree]
CONFIDENCE: High / Medium / Low (based on data completeness)
LAST CONTACT: [Date, channel, summary]

WHAT TO SAY:
"[Draft opening referencing their specific progress]"

WHY THIS APPROACH:
"[Rationale tied to their specific data, not generic]

CONTACT: [Parent name, phone, email, best time]
```

### 1.4 Hold Status: Hard Gate, Not Filter

**This is the most important rule in the system.**

A student who is on hold is NOT at risk. Their non-attendance is EXPLAINED. The hold data from Pike13 (44 West U, 22 Heights) includes explicit start/end dates — use them. Before generating ANY risk alert, the system checks hold status at the data ingestion level. An on-hold student never enters the risk pipeline.

Fixed as of July 15: Denis Tanisman (80% model risk) correctly flagged as "⏸️ ON HOLD until Jul 31 — no outreach needed."

---

## Part 2: Returning Student Re-Engagement

### 2.1 The Opportunity

Students returning from hold are at a decision point. They paused — they could easily decide not to come back. The 2 weeks before and after hold-end are the critical window.

### 2.2 Re-Engagement Intelligence

For each returning student, compile:

**Progress Continuity:**
- What were they last working on? (specific songs/pieces from notes)
- What was their last achievement? (highest score, performance, milestone)
- What's the natural next step?

**Social Connection:**
- Who was their instructor? Same one available?
- Were they in a band/rehearsal group?
- Siblings also enrolled?

**Excitement Triggers:**
- Upcoming performances or camps
- New songs/genres matching their taste (from note text)
- Milestones they were approaching
- Instructor's specific praise ("you're really getting this")

### 2.3 Three-Tier Outreach Templates

Tier chosen based on how much data we have:

**Rich Data (notes + comms + history):**
> Hi [parent], I noticed [student]'s hold ends [date]. [Instructor] was really impressed with [specific achievement] before the break — [quote]. We've got [upcoming event] on [date] that I think [student] would love. [Their] old [day/time] slot is still open. Want me to hold it?

**Moderate Data (some notes, limited comms):**
> Hi [parent], [Student]'s hold wraps up [date] — we'd love to have them back. [Instructor] had them working on [topic] and making great progress. What time works best these days?

**Sparse Data (no notes, no comms):**
> Hi [parent], just checking in as [student]'s hold ends [date]. Hope the break was great! We'd love to welcome them back — what's the best way to get them re-scheduled?

### 2.4 Hold-End Timeline

| When | Action |
|---|---|
| **14 days before** | Send re-engagement email with personal hook |
| **7 days before** | If no response, follow up via preferred channel |
| **Hold end date** | Check if re-scheduled. If not, one more outreach |
| **7 days after** | Still no response → flag for GM direct call |

---

## Part 3: Data Sources — What We Have vs What We Use

| Source | Using Now | Should Use |
|---|---|---|
| Lesson note scores | ✅ | ✅ |
| Lesson note TEXT | ❌ | Song names, challenges, breakthroughs, instructor observations |
| SMS message content | Cancel phrases only | Schedule discussions, parent tone, specific questions |
| Voicemail transcripts | Sentiment score | Specific concerns, emotional content, urgency |
| Call review transcripts | ❌ Not used | Staff notes on parent conversations, action items, promises |
| School emails | ❌ Not used | Parent-school correspondence, complaints, praise |
| Pike13 plan details | Hold dates only | Price, commitment, auto-bill, invoices, plan changes |
| Instructor-student fit | ❌ Not used | Which instructors retain best? Requested changes? |

### Data Quality Acknowledgment

Not all data is complete. Known gaps:
- Note scoring is inconsistent — some instructors score every lesson, others rarely
- SMS sender/recipient fields are empty (scraper bug) — matched via phone numbers
- Voicemail transcripts are machine-generated — expect errors
- Call reviews and emails are not yet linked to individual students
- Pike13 hold data is scraped from the live report — accuracy depends on the report

These gaps don't block the system, but they affect the *confidence* score on each recommendation. High-confidence recommendations (lots of data) can be acted on immediately. Low-confidence ones (sparse data) should be treated as a nudge to collect more information.

---

## Part 4: Implementation Roadmap

### Phase A: Data Synthesis (build first)
**Why first:** Can't classify patterns with fragmented data.
- Integrate all 8+ data sources into a unified student view
- Clean, deduplicate, and match across sources
- Acknowledge and document data gaps
- Build the "student profile" function that Phase B depends on

### Phase B: Pattern Classification
- Implement archetype detection from combined data
- Include confidence scores per classification
- Start with the 7 archetypes but allow new ones to emerge
- Generate archetype-specific intervention recommendations

### Phase C: Outcome Tracking (early — validate BEFORE scaling)
**Why early:** Don't build the full engine until you know the approach works.
- Test interventions on 10-15 students (mix of archetypes)
- Track: which interventions were tried, which students stayed
- Feed results back into archetype refinement (C → B loop)
- After 4-6 weeks of positive results, proceed to Phase D

### Phase D: Retention Brief Generator
- Auto-generate one-page briefs for every flagged student
- Pull from ALL data sources
- Include: draft outreach text, contact info, specific hooks
- Confidence score on each recommendation

### Phase E: Returning Student Engine
- Auto-detect students approaching hold-end (14, 7, 0 days)
- Generate re-engagement messages with personal hooks
- Track response rates and return rates
- This is last because the basics already work (manual emails going out now)

### Continuous Loop: C ↔ B
Outcome data continuously refines archetypes and interventions. If a "Schedule Conflict" intervention consistently fails, the archetype needs rethinking. If a new pattern emerges ("student had a baby — pausing for 6 months"), add an archetype.

---

## Part 5: What We Don't Do

1. **Don't rebuild v11.** It works correctly. Build intelligence on top of it.
2. **Don't auto-send to parents.** GM/studio coordinator makes the call. Data informs judgment, doesn't replace it.
3. **Don't chase AUC.** We already proved high AUC = wrong coefficients. Focus on actionability.
4. **Don't flag on-hold students.** Hard gate at data ingestion. Hold = explained absence.
5. **Don't overcomplicate.** Two locations, ~400 active students. Keep it simple enough that a human GM can use it in 10 minutes a week.

---

## Part 6: People & Process

This system generates recommendations for the GM and studio coordinator. For it to work:

- **Instructors** need to understand that their note quality matters — the system reads their notes to generate student hooks
- **Studio coordinator** (Calvin Barnhill, per Pike13 hold data) needs to be the primary user — they're the one scheduling and parent-facing
- **GM** reviews the report, prioritizes, and handles escalations (Comm Red Flags archetype)

The report should take <10 minutes to read and act on. If it's longer than that, it's too much.

---

## Appendix: Current State

| Component | Status |
|---|---|
| v11 prediction model | ✅ Working (6/6 signs, AUC 0.96) |
| Per-student profiles | ✅ Working (notes + comms + attendance) |
| Hold detection | ✅ Working (Pike13 scraped, 64 records) |
| Returning student section | ✅ Working (15 per school, sorted by date) |
| Cron churn emails | ⏸️ **PAUSED** (as of July 15, 3:05am CT) |
| Archetype classification | 🔜 Phase B (not yet built) |
| Intervention playbooks | 🔜 Phase B (not yet built) |
| Outcome tracking | 🔜 Phase C (not yet built) |

---

*Plan finalized July 15, 2026. Cron paused. Ready for Hugh's review when he wakes up. Next action: Hugh decides whether to proceed with Phase A or adjust the plan.*
