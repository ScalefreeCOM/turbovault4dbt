import os
def select(model_path):


        with open(os.path.join(".","templates","META_SELECT.txt"),"r") as f:
            command = f.read()
        f.close()
        model_path = model_path.replace('@@GroupName', 'Meta')
        filename = os.path.join(model_path,  f"META_SELECT.sql")
                
        path = os.path.join(model_path)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            print(f"Created Meta Model META_SELECT")


def delete(model_path):
    with open(os.path.join(".", "templates", "META_DELETE.txt"), "r") as f:
        command = f.read()
    f.close()
    model_path = model_path.replace('@@GroupName', 'Meta')
    filename = os.path.join(model_path, f"META_DELETE.sql")

    path = os.path.join(model_path)

    # Check whether the specified path exists or not
    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)

    with open(filename, 'w') as f:
        f.write(command.expandtabs(2))
        print(f"Created Meta Model META_DELETE")

def drop(model_path):
    with open(os.path.join(".", "templates", "META_DROP.txt"), "r") as f:
        command = f.read()
    f.close()
    model_path = model_path.replace('@@GroupName', 'Meta')
    filename = os.path.join(model_path, f"META_DROP.sql")

    path = os.path.join(model_path)

    # Check whether the specified path exists or not
    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)

    with open(filename, 'w') as f:
        f.write(command.expandtabs(2))
        print(f"Created Meta Model META_DROP")

def create(model_path):
    with open(os.path.join(".", "templates", "META_CREATE.txt"), "r") as f:
        command = f.read()
    f.close()
    model_path = model_path.replace('@@GroupName', 'Meta')
    filename = os.path.join(model_path, f"META_CREATE.sql")

    path = os.path.join(model_path)

    # Check whether the specified path exists or not
    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)

    with open(filename, 'w') as f:
        f.write(command.expandtabs(2))
        print(f"Created Meta Model META_CREATE")