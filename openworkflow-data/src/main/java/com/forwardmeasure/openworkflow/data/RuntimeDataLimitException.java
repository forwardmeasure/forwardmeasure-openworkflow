package com.forwardmeasure.openworkflow.data;

/** A deterministic value cannot be embedded safely in a durable boundary. */
public final class RuntimeDataLimitException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final long actualBytes;
  private final long maximumBytes;

  public RuntimeDataLimitException(long actualBytes, long maximumBytes) {
    super(
        "Inline runtime data is " + actualBytes + " bytes; maximum is " + maximumBytes + " bytes");
    if (actualBytes <= maximumBytes || maximumBytes < 1) {
      throw new IllegalArgumentException("Runtime data limit requires actual > maximum > 0");
    }
    this.actualBytes = actualBytes;
    this.maximumBytes = maximumBytes;
  }

  public long actualBytes() {
    return actualBytes;
  }

  public long maximumBytes() {
    return maximumBytes;
  }
}
