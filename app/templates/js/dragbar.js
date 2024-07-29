//  example from https://stackoverflow.com/questions/46931103/making-a-dragbar-to-resize-divs-inside-css-grids
let isLeftDragging = false;
let isRightDragging = false;

function ResetColumnSizes() {
  // when page resizes return to default col sizes
  let page = document.getElementById("grid-container");
    page.style.gridTemplateColumns = "150px auto 8px 30%";
}

function SetCursor(cursor) {
  let page = document.getElementById("grid-container");
  page.style.cursor = cursor;
}

function StartKaartDrag() {
  // console.log("mouse down");
  isLeftDragging = true;

  SetCursor("ew-resize");
}


function EndDrag() {
  // console.log("mouse up");
  isLeftDragging = false;
  isRightDragging = false;

  SetCursor("auto");
}

function OnDrag(event) {
  if (isLeftDragging || isRightDragging) {
    // console.log("Dragging");
    //console.log(event);

    let page = document.getElementById("grid-container");
    let leftcol = document.getElementById("kaart");

    let leftColWidth = isLeftDragging ? event.clientX : leftcol.clientWidth;

    let dragbarWidth = 8;

    let cols = [
      150,
      leftColWidth-150,
      dragbarWidth,
      page.clientWidth - dragbarWidth - leftColWidth,
    ];

    let newColDefn = cols.map(c => c.toString() + "px").join(" ");

    // console.log(newColDefn);
    page.style.gridTemplateColumns = newColDefn;

    event.preventDefault()
  }
}
