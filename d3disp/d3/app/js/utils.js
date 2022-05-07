module.exports = {
  // sets result .x and .y to the crossing point of two endless lines defined by (x1,y1)-(x2,y2) and (x3,y3)-(x4,y4)
  lineCrossing: function(result, x1, y1, x2, y2, x3, y3, x4, y4) {
    var px = ((x1*y2-y1*x2)*(x3-x4)-(x1-x2)*(x3*y4-y3*x4))/((x1-x2)*(y3-y4)-((y1-y2)*(x3-x4)));
    var py = ((x1*y2-y1*x2)*(y3-y4)-(y1-y2)*(x3*y4-y3*x4))/((x1-x2)*(y3-y4)-((y1-y2)*(x3-x4)));
    result.x = px;
    result.y = py;
  },

  // a line goes from (p.x,p.y) to (rp.x,rp.y) that is inside the rectangle (r.x, r.y, r.width, r.height)
  // adjusts (rp.x,rp.y) along the line, to be at the edge of r
  adjustToRectEdge: function(p, rp, r) {
    var x3, y3, x4, y4;
    var dx = rp.x - p.x;
    var dy = rp.y - p.y;
    var k = dx == 0 ? 1000000 : dy / dx;
    var rk = r.height / r.width;
    x3 = r.x;
    y3 = r.y;
    if (Math.abs(k) < Math.abs(rk)) {
      // line crosses left or right rect edge
      x4 = r.x;
      y4 = r.y + r.height;
      if (dx < 0) {
        // line crosses right edge
        x3 += r.width;
        x4 += r.width;
      }
    } else {
      // line crosses top or bottom rect edge
      x4 = r.x + r.width;
      y4 = r.y;
      if (dy < 0) {
        // line crosses bottom edge
        y3 += r.height;
        y4 += r.height;
      }
    }
    this.lineCrossing(rp, p.x, p.y, rp.x, rp.y, x3, y3, x4, y4);
  },
};
