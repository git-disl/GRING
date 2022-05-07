import * as d3 from './d3';
import GraphRenderer from './graph-renderer';

let gr = new GraphRenderer('#graph');

gr.onClick(function(d) {
    console.log('click:', d);
});

gr.setMenu([
  {
    title: 'Item #1',
    action: function(elm, d, i) {
      console.log('Item #1 clicked!');
      console.log('The data is: ', d);
    },
    disabled: false // optional, defaults to false
  },
  {
    title: 'Item #2',
    action: function(elm, d, i) {
      console.log('You have clicked the second item!');
      console.log('The data is: ', d);
    }
  }
]);

d3.json('model.json', function(error, data) {
  gr.render(data)
});
