import ast, os, sys
from collections import deque

def parse_imports(path):
    try:
        tree = ast.parse(open(path, "r", encoding="utf8").read(), path)
    except Exception:
        return set()
    mods = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            for alias in n.names:
                mods.add(alias.name)
        elif isinstance(n, ast.ImportFrom):
            mod = n.module if n.module else ""
            level = n.level
            mods.add(("." * level) + mod if level else mod)
    return mods

def resolve_mod_to_path(mod, src_file, root):
    # handle relative like '..module'
    if mod.startswith("."):
        pkg = os.path.dirname(src_file)
        up = mod.count(".")
        rel = mod.lstrip(".")
        for _ in range(up-1 if up>0 else 0):
            pkg = os.path.dirname(pkg)
        base = pkg
        if rel:
            base = os.path.join(base, rel.replace(".", os.sep))
        candidates = [base + ".py", os.path.join(base, "__init__.py")]
    else:
        base = os.path.join(root, *mod.split("."))
        candidates = [base + ".py", os.path.join(base, "__init__.py")]
    for c in candidates:
        if os.path.isfile(c):
            return os.path.normpath(c)
    return None

def find_deps(entry, root):
    entry = os.path.abspath(entry)
    root = os.path.abspath(root)
    q = deque([entry])
    seen = set()
    edges = []
    while q:
        f = q.popleft()
        if f in seen: continue
        seen.add(f)
        for mod in parse_imports(f):
            p = resolve_mod_to_path(mod, f, root)
            if p and p.startswith(root):
                edges.append((f, p))
                q.append(p)
    return seen, edges

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python find_deps.py <entry_file> [project_root]")
        sys.exit(1)
    entry = sys.argv[1]
    root = sys.argv[2] if len(sys.argv)>2 else os.path.dirname(os.path.dirname(entry))
    files, edges = find_deps(entry, root)
    print("Files discovered:")
    for f in sorted(files):
        print(" ", f)
    print("\nImport edges:")
    for a,b in edges:
        print(" ", a, "->", b)