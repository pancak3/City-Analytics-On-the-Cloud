// https://stackoverflow.com/questions/217578
var doc = {
    "_id": "1018309867130576896",
    "_rev": "1-2126b5748cdc558111994171f8621e16",
    "created_at": "Sun Jul 15 01:42:46 +0000 2018",
    "id": 1018309867130576900,
    "id_str": "1018309867130576896",
    "text": "Run a #wuu2k aid station they said. It'll be fun, they said. And it WAS! \n\nThink I now have a volunteer's high, is… https://t.co/jij2SHPbaR",
    "truncated": true,
    "entities": {
        "hashtags": [
            {
                "text": "wuu2k",
                "indices": [
                    6,
                    12
                ]
            }
        ],
        "symbols": [],
        "user_mentions": [],
        "urls": [
            {
                "url": "https://t.co/jij2SHPbaR",
                "expanded_url": "https://twitter.com/i/web/status/1018309867130576896",
                "display_url": "twitter.com/i/web/status/1…",
                "indices": [
                    116,
                    139
                ]
            }
        ]
    },
    "source": "<a href=\"http://instagram.com\" rel=\"nofollow\">Instagram</a>",
    "in_reply_to_status_id": null,
    "in_reply_to_status_id_str": null,
    "in_reply_to_user_id": null,
    "in_reply_to_user_id_str": null,
    "in_reply_to_screen_name": null,
    "user": {
        "id": 114673850,
        "id_str": "114673850",
        "name": "Heather M",
        "screen_name": "heatherwgtn",
        "location": "Wellington, New Zealand",
        "description": "Wellingtonian | Comms & SM Adviser  | Trail runner and Jogsquader | Dog parent",
        "url": "https://t.co/tEjz063RBb",
        "entities": {
            "url": {
                "urls": [
                    {
                        "url": "https://t.co/tEjz063RBb",
                        "expanded_url": "http://heatherontherun.wordpress.com/",
                        "display_url": "heatherontherun.wordpress.com",
                        "indices": [
                            0,
                            23
                        ]
                    }
                ]
            },
            "description": {
                "urls": []
            }
        },
        "protected": false,
        "followers_count": 814,
        "friends_count": 987,
        "listed_count": 31,
        "created_at": "Tue Feb 16 07:00:07 +0000 2010",
        "favourites_count": 700,
        "utc_offset": null,
        "time_zone": null,
        "geo_enabled": true,
        "verified": false,
        "statuses_count": 2366,
        "lang": null,
        "contributors_enabled": false,
        "is_translator": false,
        "is_translation_enabled": false,
        "profile_background_color": "642D8B",
        "profile_background_image_url": "http://abs.twimg.com/images/themes/theme10/bg.gif",
        "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme10/bg.gif",
        "profile_background_tile": true,
        "profile_image_url": "http://pbs.twimg.com/profile_images/925806128671334400/Wk5ajLu6_normal.jpg",
        "profile_image_url_https": "https://pbs.twimg.com/profile_images/925806128671334400/Wk5ajLu6_normal.jpg",
        "profile_banner_url": "https://pbs.twimg.com/profile_banners/114673850/1484265603",
        "profile_link_color": "3B94D9",
        "profile_sidebar_border_color": "FFFFFF",
        "profile_sidebar_fill_color": "DDEEF6",
        "profile_text_color": "333333",
        "profile_use_background_image": true,
        "has_extended_profile": false,
        "default_profile": false,
        "default_profile_image": false,
        "following": false,
        "follow_request_sent": false,
        "notifications": false,
        "translator_type": "none"
    },
    "geo": {
        "type": "Point",
        "coordinates": [
            -41.34522562,
            174.76700788
        ]
    },
    "coordinates": {
        "type": "Point",
        "coordinates": [
            174.76700788,
            -41.34522562
        ]
    },
    "place": {
        "id": "013b5456649606dc",
        "url": "https://api.twitter.com/1.1/geo/id/013b5456649606dc.json",
        "place_type": "city",
        "name": "Wellington City",
        "full_name": "Wellington City, New Zealand",
        "country_code": "NZ",
        "country": "New Zealand",
        "contained_within": [],
        "bounding_box": {
            "type": "Polygon",
            "coordinates": [
                [
                    [
                        174.613267,
                        -41.362455
                    ],
                    [
                        174.89541,
                        -41.362455
                    ],
                    [
                        174.89541,
                        -41.14354
                    ],
                    [
                        174.613267,
                        -41.14354
                    ]
                ]
            ]
        },
        "attributes": {}
    },
    "contributors": null,
    "is_quote_status": false,
    "retweet_count": 0,
    "favorite_count": 11,
    "favorited": false,
    "retweeted": false,
    "possibly_sensitive": false,
    "lang": "en",
    "stream_status": false,
    "direct_stream": false
}
if (doc.coordinates.type == 'Point') {
    var p = doc.coordinates.coordinates;
    var locations = geo['features']

    for (const location of locations) {
        var geometry = location["geometry"];
        if (geometry.type === "MultiPolygon") {
            var location_name = location["properties"]["feature_name"];
            for (var polygons of geometry["coordinates"]) {
                for (var polygon of polygons) {
                    var minX = polygon[0][0], maxX = polygon[0][0];
                    var minY = polygon[0][1], maxY = polygon[0][1];
                    for (var n = 1; n < polygon.length; n++) {
                        var q = polygon[n];
                        minX = Math.min(q[0], minX);
                        maxX = Math.max(q[0], maxX);
                        minY = Math.min(q.y, minY);
                        maxY = Math.max(q.y, maxY);
                    }

                    // if (p[0] < minX || p[0] > maxX || p[1] < minY || p[1] > maxY) {
                    //     //Not in this polygon
                    //     // emit(doc._id, doc.coordinates.coordinates);
                    //     continue;
                    // }
                    var isInside = false;
                    var i = 0, j = polygon.length - 1;
                    for (i, j; i < polygon.length; j = i++) {
                        if ((polygon[i][1] > p[1]) != (polygon[j][1] > p[1]) &&
                            p[0] < (polygon[j][0] - polygon[i][0]) * (p[1] - polygon[i][1]) / (polygon[j][1] - polygon[i][1]) + polygon[i][0]) {
                            isInside = !isInside;
                        }
                    }
                    if (isInside) {
                        console.log(doc._id, location_name)
                        // emit(doc._id, location_name);
                    }
                }

            }
        }
    }
}