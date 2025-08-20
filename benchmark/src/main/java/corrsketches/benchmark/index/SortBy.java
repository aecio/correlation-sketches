package corrsketches.benchmark.index;

public enum SortBy {
  KEY(null),
  CSK(new Hit.CorrelationSketchReranker()),
  BAL(new Hit.Corr1Join1SketchReranker()),
  GRD(new Hit.GreedyDiverseSketchReranker()),
  GR2(new Hit.GreedyDiverse2SketchReranker()),
  GR3(new Hit.GreedyDiverse3SketchReranker());

  public Hit.RerankStrategy reranker;

  SortBy(Hit.RerankStrategy reranker) {
    this.reranker = reranker;
  }
}
