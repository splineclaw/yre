// https://codepen.io/desandro/pen/MwJoZQ
var grid = document.querySelector('.grid');

var msnry = new Masonry( grid, {
  itemSelector: '.grid-item',
  columnWidth: '.grid-sizer',
  percentPosition: true,
  columnWidth: 0,
});

imagesLoaded( grid ).on( 'progress', function() {
  // layout Masonry after each image loads
  msnry.layout();
});
