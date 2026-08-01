export class LineBuffer {
  private buffer = '';

  addData(data: string): string[] {
    this.buffer += data;
    const lines: string[] = [];
    let newlineIndex: number;
    while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, newlineIndex).trim();
      this.buffer = this.buffer.slice(newlineIndex + 1);
      if (line.length > 0) lines.push(line);
    }
    return lines;
  }

  getRemaining(): string {
    return this.buffer;
  }

  clear(): void {
    this.buffer = '';
  }
}
