
require 'net/http'
require 'rails'
require 'elasticsearch'
require 'byebug'

require_relative 'my_time'

SOURCE = {
	:host => "10.1.7.212",
	:port => "9200",
	:index_name => "buy",
	:type_name => "inventory"
}
TARGET = {
	:host => "10.1.7.212",
	:port => "9200",
	:index_name => "benchmark_index",
	:type_name => "inventory"
}
BATCH_SIZE = 1000

def post_es_response(host, port, extension, query = "")
	uri = URI("http://#{host}:#{port}#{extension}")
	req = Net::HTTP::Post.new(uri, initheader = {'Content-Type' =>'application/json'})
    req.body = query
    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
    	response = http.request(req)
    end
end

def get_es_response(host, port, extension)
	Net::HTTP.get(URI.parse("http://#{host}:#{port}#{extension}"))	
end

def get_extension node
	"/#{node[:index_name]}/#{node[:type_name]}/_search"
end

def get_client(node)
  Elasticsearch::Client.new(host: node[:host],port: node[:port])
end

# Return the No of docs from the Index
def get_index_size node
	query = { :size => 0 }
	extension = "/#{node[:index_name]}/#{node[:type_name]}/_search"
	res = post_es_response( node[:host], node[:port], extension, query.to_json )
	res = JSON.parse(res.body)
	res["hits"]["total"]
end

def create_index node
	extension = "/#{node[:index_name]}"
	query = ""
	post_es_response( node[:host], node[:port], extension, query.to_json )
end

def get_setting node
	extension = "/#{node[:index_name]}/_settings"
	query = ""
	res = get_es_response( node[:host], node[:port], extension)
	res = JSON.parse(res)
	res[node[:index_name]]
end

def get_mapping node
	extension = "/#{node[:index_name]}/_mapping"
	query = ""
	res = get_es_response( node[:host], node[:port], extension)
	res = JSON.parse(res)
	res[node[:index_name]]
end

def put_mapping node, mapping, setting
	extension = "/#{node[:index_name]}"
	uri = URI("http://#{node[:host]}:#{node[:port]}#{extension}")
	req = Net::HTTP::Post.new(uri, initheader = {'Content-Type' =>'application/json'})
	setting["settings"]["index"]["number_of_shards"] = "5"
	setting["settings"]["index"]["number_of_replicas"] = "0"
    req.body = { :mappings => mapping["mappings"], :settings => setting["settings"] }.to_json
    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
    	response = http.request(req)
    end
end

def index_transfer
	size = get_index_size(SOURCE)
	no_of_batches = (size/BATCH_SIZE)

	start_time = Time.now
	time = MyTime.new

	# put mapping from source node to target node.
	source_mapping = get_mapping SOURCE
	source_setting = get_setting SOURCE
	put_mapping(TARGET, source_mapping, source_setting)

	(0..no_of_batches).each do |batch_no|
		query = {}
		query[:from] = batch_no*BATCH_SIZE
		query[:size] = BATCH_SIZE
		source_extension = get_extension SOURCE
		res = post_es_response( SOURCE[:host], SOURCE[:port], source_extension, query.to_json )
		res = JSON.parse(res.body) rescue next
		body = []
		data = res["hits"]["hits"].map { |t| t["_source"] }
		data.each do |d|
			doc = {"index" => {"_index" => TARGET[:index_name],"_type" => TARGET[:type_name],"_id" => "#{d['id']}_#{d['type']}", "data" => d}}
			body.push(doc)
		end
		target_client = get_client TARGET
		response = target_client.bulk body: body
		if response[:errors] == true
      raise "batch_no. #{batch_no} -- index with errors as #{(response["items"].map{|t| t["index"]["error"]}.compact)}"
    end
    puts "Indexed batch no. #{batch_no} with #{BATCH_SIZE} docs from #{SOURCE[:host]}-(index_name: #{SOURCE[:index_name]}) to #{TARGET[:host]}-(index_name: #{TARGET[:index_name]})"
    time.remaining_time((batch_no+1)*BATCH_SIZE, size) if batch_no%10 == 0
	end
	puts "====================================================================================================================================="
	puts "Index Sucessfully transfered. -- from #{SOURCE[:host]}-#{SOURCE[:index_name]} to #{TARGET[:host]}-#{TARGET[:index_name]}"
	time.time_taken
end