class Dashing.Number2 extends Dashing.Widget
  @accessor 'num1', Dashing.AnimatedValue
  @accessor 'num2', Dashing.AnimatedValue



  @accessor 'arrow', ->
    if @get('last1')
      if parseInt(@get('current1')) > parseInt(@get('last1')) then 'icon-arrow-up' else 'icon-arrow-down'

  onData: (data) ->
    if data.status
      $(@get('node')).addClass("status-#{data.status}")
