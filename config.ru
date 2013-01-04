require 'openid/store/filesystem'
require 'omniauth/strategies/google_apps'
require 'dashing'

$admins = ['alina.valero@birchbox.com', 'katia@birchbox.com', 'hayley@birchbox.com', 'liz@birchbox.com', 'matt.field@birchbox.com', 'quentin.vacher@joliebox.com', 'martin.balas@joliebox.com', 'benjamin@birchbox.com', 'michael@birchbox.com', 'deena@birchbox.com' ]

configure do
  set :auth_token, 'YOUR_AUTH_TOKEN'
  set :default_dashboard, 'landing'	

DBEU = Sequel.connect(:adapter => 'mysql2', :user => 'sequelpro', :host => '127.0.0.1', :database => 'joliebox',:password=>'2hLGlwfpdaJO', :port => 44445)
DB = Sequel.connect(:adapter => 'mysql2', :user => 'benjamin', :host => '127.0.0.1', :database => 'magento_birchbox',:password=>'gxU9Jn8Vhv39hJMx', :port => 44444)

  helpers do
    def protected!
       redirect '/login' unless session[:user_id] || request.path_info == '/login'
     # Put any authentication code you want in here.
     # This method is run before accessing any resource.
    end

    def is_admin
       if $admins.include? session[:user_id]
         return true
       else
         return false
       end
     end

     def is_joliebox
       if session[:domain] == 'joliebox'
         return true
       else
         return false
       end
     end

     def is_birchbox
       if session[:domain] == 'birchbox'
         return true
       else
         return false
       end
     end

     def allow_joliebox
       if not is_admin
         if not is_joliebox
           redirect '/permission_denied'
         end
       end
     end

     def allow_birchbox
       if not is_admin
         if not is_birchbox
           redirect '/permission_denied'
         end
       end
     end

     def show_for_birchbox
       if is_birchbox || is_admin
         true
       else
         false
       end
     end

     def show_for_joliebox
       if is_joliebox || is_admin
         true
       else
         false
       end
     end

  use Rack::Session::Cookie
  use OmniAuth::Builder do
    provider :google_apps, :store => OpenID::Store::Filesystem.new('./tmp'), :name => 'birchbox', :domain => 'birchbox.com'
    provider :google_apps, :store => OpenID::Store::Filesystem.new('./tmp'), :name => 'joliebox', :domain => 'joliebox.com'
    provider :google_apps, :store => OpenID::Store::Filesystem.new('./tmp'), :name => 'joliebox-es', :domain => 'joliebox.es'
    provider :google_apps, :store => OpenID::Store::Filesystem.new('./tmp'), :name => 'joliebox-uk', :domain => 'joliebox.co.uk'
  end
  
  post '/auth/birchbox/callback' do
    if auth = request.env['omniauth.auth']
      session[:user_id] = auth['info']['email']
      session[:domain] = 'birchbox'
      redirect '/'
    else
      redirect '/auth/failure'
    end
  end

  post '/auth/joliebox/callback' do
    if auth = request.env['omniauth.auth']
      session[:user_id] = auth['info']['email']
      session[:domain] = 'joliebox'
      redirect '/'
    else
      redirect '/auth/failure'
    end
  end

  post '/auth/joliebox-es/callback' do
    if auth = request.env['omniauth.auth']
      session[:user_id] = auth['info']['email']
      session[:domain] = 'joliebox'
      redirect '/'
    else
      redirect '/auth/failure'
    end
  end

  post '/auth/joliebox-uk/callback' do
    if auth = request.env['omniauth.auth']
      session[:user_id] = auth['info']['email']
      session[:domain] = 'joliebox'
      redirect '/'
    else
      redirect '/auth/failure'
    end
  end

  get '/auth/failure' do
    'Nope.'
  end

  get '/permission_denied' do
     'Permission denied.'
  end
  end 
end

map Sinatra::Application.assets_prefix do
  run Sinatra::Application.sprockets
end

run Sinatra::Application
