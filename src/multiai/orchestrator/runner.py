def run():
    print("Hello-autonomy task executed locally.")
    with open("docs/hello_autonomy.md","a") as f:
        f.write("\nHello autonomy executed.\n")
    return True
