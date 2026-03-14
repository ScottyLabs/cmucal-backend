def insert_agent_run(db, agent_version: str) -> int:
    """ Insert a new agent run and return its ID """
    res = db.table("agent_runs").insert({"agent_version": agent_version}).execute()
    return res.data[0]["id"]