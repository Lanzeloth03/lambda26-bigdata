"""Ejecuta solo un tramo de celdas [inicio, fin) de un cuaderno, en un kernel nuevo, y guarda sus salidas en el mismo archivo.
Uso (dentro del contenedor): python ejecutar_cuaderno_parte.py /opt/s12-ecodata/12_ecodata_u2.ipynb 134 157"""
import os, sys, nbformat
from nbclient import NotebookClient

ruta, ini, fin = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
nb = nbformat.read(ruta, as_version=4)
sub = nbformat.v4.new_notebook()
sub.metadata = nb.metadata
sub.cells = nb.cells[ini:fin]
NotebookClient(sub, timeout=3600, allow_errors=True,
               kernel_name=nb.metadata.get("kernelspec", {}).get("name", "python3"),
               resources={"metadata": {"path": os.path.dirname(os.path.abspath(ruta))}}).execute()
nb.cells[ini:fin] = sub.cells
nbformat.write(nb, ruta)
print(f"Celdas {ini}-{fin - 1} ejecutadas y guardadas en {ruta}")
