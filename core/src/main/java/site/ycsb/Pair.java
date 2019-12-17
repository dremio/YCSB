package site.ycsb;

import java.util.Objects;

public class Pair {
  private Status status;
  private Object version;

  public Pair(Status status, Object version) {
    this.status = status;
    this.version = version;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public Object getVersion() {
    return version;
  }

  public void setVersion(Object version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Pair pair = (Pair) o;
    return status.equals(pair.status) &&
        version.equals(pair.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, version);
  }
}
