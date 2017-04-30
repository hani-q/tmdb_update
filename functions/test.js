function testing() {
     client.get(url, function (data, response) {

        console.log('Total Pages ' + data.total_pages);
        console.log('Started fetching Movies');
        // Calling fucntion to get datail


        var last_end = 1;
        var end = 1;

        for(var i = 1; i <= data.total_pages; ) {
            if(i + page_width > data.total_pages)
                end = data.total_pages;
            else
                end = i + page_width - 1;

            console.log(`Fetching from ${i} - ${end}`);
            get_data(i, end, results);

            i = i + page_width;
            // last_end = end;
        }
        return;
    });
}

function results (page) {
    console.log('Total Pages so far = ' + Object.keys(global.data).length);
    var moviesRef = defaultDatabase.ref("v1/movies");
    //moviesRef.remove();
    var data_json = {};
    for (var i = 0; i < global.data.length; i++) {
        for (var j = 0; j < global.data[i].length; j ++) {
            var res_array = global.data[i];
            var toon = res_array[j];
            data_json[toon.id] = toon;
        }
        break;
    }
    moviesRef.update(data_json);
    moviesRef.off();
    console.log("finished fetching movies for job" + page);
    return;
}

testing();
