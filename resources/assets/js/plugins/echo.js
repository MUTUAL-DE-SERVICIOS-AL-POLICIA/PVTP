import Echo from 'laravel-echo'

// window.io = require('socket.io-client')
// window.Echo = new Echo({
//   broadcaster: 'socket.io',
//   host: `${process.env.MIX_LARAVEL_ECHO_SERVER_HOST}:${process.env.MIX_LARAVEL_ECHO_SERVER_PORT}`
// })
// window.Echo.connector.socket.on('connect', () => {
//   console.log(`Connected to echo server with ID: ${window.Echo.socketId()}`)
// })
// window.Echo.connector.socket.on('disconnect', () => {
//   console.log('Disconnected from echo server')
// })
// window.Echo.connector.socket.on('reconnecting', attemptNumber => {
//   console.log(`Reconnecting to echo server, try #${attemptNumber}`)
// })