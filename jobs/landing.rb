us_boards = { 'men' => {text: 'Men', page: 'us-men'}, 'women' => {text: 'Women', page: 'us-women'}, 'summary' => {text: 'Summary', page: 'us-summary'} }
uk_boards = { 'women' => {text: 'Women', page: 'uk'} }
fr_boards = { 'women' => {text: 'Women', page: 'france'} }
sp_boards = { 'women' => {text: 'Women', page: 'es'} }

send_event('us', { items: us_boards.values })
send_event('uk', { items: uk_boards.values })
send_event('fr', { items: fr_boards.values })
send_event('sp', { items: sp_boards.values })


