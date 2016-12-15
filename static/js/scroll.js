$(document).ready(function () {
    // Handler for .ready() called.
    console.log('test');
    console.log(count);
    var list={{list|tojson}};
    if (list) {
	    $('html, body').animate({
	        scrollTop: $('#top_movie_result').offset().top}, 'slow');
    };
});