package struct;

public class PageHeader {
  private final BTreePageType pageType;
  private final int cellsCount;

  public PageHeader(BTreePageType pageType, int cellsCount) {
    this.pageType = pageType;
    this.cellsCount = cellsCount;
  }

  public BTreePageType getPageType() {
    return this.pageType;
  }

  public int getCellsCount() {
    return this.cellsCount;
  }
}
