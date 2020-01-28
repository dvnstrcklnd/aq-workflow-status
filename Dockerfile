FROM jupyter/scipy-notebook as notebook-base

RUN pip install pydent \
    && pip install plotly \
    && pip install ipywidgets
