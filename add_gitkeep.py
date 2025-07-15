import os

for root, dirs, files in os.walk("."):
    for d in dirs:
        dir_path = os.path.join(root, d)
        if not os.listdir(dir_path):  # folder is empty
            with open(os.path.join(dir_path, ".gitkeep"), "w") as f:
                pass