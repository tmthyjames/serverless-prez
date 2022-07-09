from pathlib import Path
import requests


def get_project_root() -> Path:
    return Path(__file__).parent.parent

def download_url(url, save_path, chunk_size=128):
    save_path = Path(save_path)
    save_path_parent = save_path.parent
    save_path_parent.mkdir(parents=True, exist_ok=True)
    file = save_path.name
    r = requests.get(url, stream=True)
    with open(str(save_path_parent/file), 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

def make_archive(source: str='../src/aws/lambda.py'):
    source = Path(source)
    destination = source.with_suffix('.zip')
    os.remove(destination) if os.path.exists(destination) else None
    base = os.path.basename(destination)
    name = source.stem
    format = destination.suffix.replace('.', '')
    archive_from = os.path.dirname(source)
    archive_to = os.path.basename(source.parts[-1])
    print(name, format, archive_from, archive_to)
    shutil.make_archive(name, format, archive_from, archive_to)
    shutil.move(f'{name}.{format}', destination)
    return destination
