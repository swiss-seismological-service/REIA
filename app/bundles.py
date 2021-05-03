from flask_assets import Bundle

bundles = {
    'css_bundle': Bundle(
        'css/styles.css',
        output='gen/styles.css',
        filters='cssmin'
    ),
    'js_bundle': Bundle(
        'js/scripts.js',
        output='gen/scripts.js',
        filters='jsmin'
    )
}
