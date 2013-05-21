<?php
header("Expires: Mon, 26 Jul 2005 05:00:00 GMT");
echo "<head> </head>";
echo "<html><br>html part<br></html>";
if (isset($_GET["variable"])) {
  echo "mi hai passato".$_GET["variable"];
} else {
  echo "non mi hai passato niente";
}
?>

