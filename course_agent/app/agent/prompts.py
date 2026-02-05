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
Is this clearly the official course website (not Piazza, Canvas, Reddit, or a generic page)?

Answer ONLY one word: accept or reject.
"""

