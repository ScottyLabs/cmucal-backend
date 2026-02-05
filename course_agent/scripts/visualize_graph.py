from course_agent.app.agent.graph import build_course_agent

def main():
    graph = build_course_agent()

    # Generate Graphviz object
    dot = graph.get_graph().draw_mermaid()

    # Save to file
    with open("course_agent_graph.mmd", "w") as f:
        f.write(dot)

    print("Mermaid graph saved to course_agent_graph.mmd")

if __name__ == "__main__":
    main()
