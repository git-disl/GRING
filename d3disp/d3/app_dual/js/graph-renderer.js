import * as d3 from './d3';
import _ from 'lodash';
import contextMenuFactory from 'd3-context-menu';

export default function(graphSite) {
  let r = 20,
    width = 800,
    height = 300,
    clickHandler,
    menu;

  let contextMenu = contextMenuFactory(d3);


  var svg = d3.select(graphSite).append('svg')
  .attr('width', width).attr('height', height);

  svg.append('defs').append('marker')
  .attr("id", 'markerArrowEnd')
  .attr("viewBox", "0 -5 10 10")
  .attr("refX", 10)
  .attr("refY", 0)
  .attr("markerWidth", 8)
  .attr("markerHeight", 8)
  .attr("orient", "auto")
  .append("path")
  .attr("d", 'M0,-5 L10,0 L0,5')
  .attr('fill', 'black');

  function render(data) {
    // run autolayout
    var simulation = d3.forceSimulation(data.nodes)
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force("charge", d3.forceManyBody().distanceMin(r * 2))
    .force("link", d3.forceLink(data.links).id(d => d.id).distance(70))
    .force("collide",d3.forceCollide())
    .on("tick", tick);

    // render nodes
    var nodes = svg.selectAll('.node').data(data.nodes, function(d) {return d.id});
    nodes.exit().remove();
    var nodesEnter = nodes.enter().append('g')
    .attr('class', 'node')
    .attr('transform', 'translate(50, 50)');
    nodesEnter.append('circle')
    .attr('r', r);
    nodesEnter.append('text');
    var enterUpdate = nodesEnter.merge(nodes);
    enterUpdate.select('text').text(function(d) {return d.label});
    enterUpdate.select('text').each(function(d) {
      var bbox = this.getBBox();
      d3.select(this).attr('x', -bbox.x - bbox.width / 2);
      d3.select(this).attr('y', -bbox.y - bbox.height / 2);
    });
    enterUpdate.on('click', function(d) {
        if (clickHandler) {
          clickHandler(d);
        }
    });
    console.log('menu',menu);
    if (menu) {
      enterUpdate.on('contextmenu', contextMenu(menu));
    }

    // render links
    var links = svg.selectAll('.link').data(data.links, function(d) {return String(d.source) + '_' + String(d.target)});
    links.exit().remove();
    var linksEnter = links.enter().append('line')
    .attr('class', 'link')
    .attr('marker-end', 'url(#markerArrowEnd)');
    var linksEnterUpdate = linksEnter.merge(links);

    // returns {from: {x, y}, to: {x, y}} that goes between
    // edge of circles with the supplied center points
    function adjustEnds(fromCenterPoint, toCenterPoint, r) {
      var dx = toCenterPoint.x - fromCenterPoint.x,
      dy = toCenterPoint.y - fromCenterPoint.y,
      length = Math.sqrt(dx * dx + dy * dy);
      dx = dx / length * r;
      dy = dy / length * r;
      return {from: {x: fromCenterPoint.x + dx, y: fromCenterPoint.y + dy}, to: {x: toCenterPoint.x - dx, y: toCenterPoint.y - dy}};
    }

    // autolayout animation
    function tick() {
      enterUpdate.each(function(d) {
        d3.select(this).attr('transform', 'translate(' + d.x + ', ' + d.y + ')');
      });
      linksEnterUpdate.each(function(d) {
        var adjustedEnds = adjustEnds(d.source, d.target, r);
        d3.select(this)
        .attr("x1", function(d) { return adjustedEnds.from.x; })
        .attr("y1", function(d) { return adjustedEnds.from.y; })
        .attr("x2", function(d) { return adjustedEnds.to.x; })
        .attr("y2", function(d) { return adjustedEnds.to.y; });
      });
    }
  }

  function onClick(aClickHandler) {
    clickHandler = aClickHandler;
  }

  function setMenu(aMenu) {
    console.log('setmenu',aMenu);
    menu = aMenu;
  }

  return {
    render: render,
    onClick: onClick,
    setMenu: setMenu
  };
}
