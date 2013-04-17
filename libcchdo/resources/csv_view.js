$(document).ready(function(){
  // https://github.com/Mottie/tablesorter/issues/186
  $.tablesorter.addWidget({
    id: 'col-reorder',
    init: function(table, thisWidget) {
      var wo = table.config.widgetOptions;
      $(table).find('thead th').on('mouseup', function() {
         var endNdx=$(this).index();
         if((wo.startColNdx != endNdx) && (wo.startColNdx >-1) && (endNdx>-1)) {
            var rows = jQuery('thead tr, tbody tr', $(table));
            var cols,colFrom,colTo;
            if((wo.startColNdx<endNdx) && ((endNdx-wo.startColNdx))==1) {
               colFrom = endNdx;
               colTo = wo.startColNdx;
            }else {
               colFrom = wo.startColNdx;
               colTo = endNdx;
            }
            rows.each(function() {
               cols = jQuery(this).children('th, td');
               cols.eq(colFrom).detach().insertBefore(cols.eq(colTo));
            });
         }
         wo.startColNdx=-1;
      }).on('mousedown', function(){
         wo.startColNdx = $(this).index();
      });
    },
    format: function(table, init){},
    remove: function(table, c, wo){}
  });

  $('#data-table').tablesorter({
    theme: "blue",
    widgets: ['stickyHeaders', 'col-reorder'],
    widgetsOptions: {
      stickyHeaders: ['tablesorter-stickyHeader'],
    }
  });
  // Toggle row selection
  $('#data-table tr').on('click', function() {
    $(this).toggleClass('selected');
  });
});
