import os


def safe_mkdir(path: str) -> None:
    if not os.path.exists(path):
        os.mkdir(path)


def check_paths(input: str, output: str, is_output_dir: bool=False) -> None:
    if input is not None and not os.path.exists(input):
        raise FileNotFoundError(f"Input path {os.path.abspath(input)} not found")

    if is_output_dir:
        output_dirname = os.path.abspath(output)
    else:
        output_dirname = os.path.dirname(os.path.abspath(output))
        

    parent_dirname = os.path.dirname(output_dirname)
    if not os.path.exists(parent_dirname):
        raise FileNotFoundError(
            f"Parent path {parent_dirname} not found"
        )

    if not os.path.exists(output_dirname):
        print(f"Warning: dirname {output_dirname}, creating it")
        safe_mkdir(output_dirname)
