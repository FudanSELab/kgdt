from kgdt.pipeline.base import PipelineListener


class GraphSavePipelineListener(PipelineListener):
    def __init__(self, graph_path):
        self.graph_path = graph_path

    def on_before_run_component(self, component_name, kg_build_pipeline, **config):
        pass

    def on_after_run_component(self, component_name, kg_build_pipeline, **config):
        print("hook after pipeline run component %r" % component_name)
        kg_build_pipeline.save_graph(self.graph_path)


class DocumentSavePipelineListener(PipelineListener):
    def __init__(self, doc_path):
        self.doc_path = doc_path

    def on_before_run_component(self, component_name, kg_build_pipeline, **config):
        pass

    def on_after_run_component(self, component_name, kg_build_pipeline, **config):
        print("hook after pipeline run component %r" % component_name)
        kg_build_pipeline.save_doc(self.doc_path)
