task :default => [:test]
task :test do
  Dir.foreach('test') do |testfile|
    next if testfile =~ /^\./
    ruby "test/#{testfile}"
  end
end