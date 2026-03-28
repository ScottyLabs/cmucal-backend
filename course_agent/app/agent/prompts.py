VERIFY_SITE_PROMPT = """
You are verifying whether a webpage is the primary course website that students would use.

Course: {course_name}
Page URL: {url}

Page snippet:
{text}

Guidelines:
- Many official CMU course websites are hosted on:
  - professor homepages (e.g., cs.cmu.edu/~username)
  - GitHub Pages (e.g., *.github.io)
- Sparse or unpolished design is normal.
- A page is an official course website if it contains
  course-specific information such as:
  syllabus, schedule, lectures, assignments, office hours, or staff.
- Do NOT require CMU branding or logos.
- Answer "no" ONLY if the page is clearly:
  Piazza, Canvas, Reddit, StackOverflow, a forum, or unrelated.
- Semester relevance rule:
  - If the snippet explicitly indicates a different semester/year than the current offering,
    answer "no".
  - If semester/year is ambiguous or missing, do not reject based on semester alone.
  - Do not trust generic "current semester" wording unless an explicit term/year is present.

Question:
Is this likely the primary website for this course?

Answer ONLY one word: yes or no.
"""


CRITIC_PROMPT = """
You are reviewing another agent's decision.

Course: {course_name}
Proposed website: {url}

Page content:
{text}

Question:
Is this clearly the official course website (not Piazza, Canvas, Reddit, or a generic page),
and not explicitly for a different semester/year?

Guidelines:
- Reject if the page explicitly references a different semester/year from the current offering.
- If semester/year is ambiguous or missing, do not reject on semester alone.
- Do not rely on generic "current semester" claims without explicit term/year evidence.

Answer ONLY one word: accept or reject.
"""

